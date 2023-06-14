"""
serial_dlc02.py - Driver for the Mean Well DLC-02 controller

The DLC-02 actually uses Modbus TCP for communication, but this is pretty
similar to using Modbus over RS-485 and so the serial driver base is the best
match.

Note that one very large caveat of using the DLC-02 is that it DOES NOT support
listening to raw DALI commands over the bus. The best that it can do is report
a sequence of specific events that the DLC-02 decodes itself these are limited
to: push button, absolute input "slider", occupancy, and light intensity. This
driver does not (yet) attempt to listen for these events and translate them
back into what the DALI messages would have been.


This file is part of python-dali.

python-dali is free software: you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
details.

You should have received a copy of the GNU Lesser General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import reduce
from operator import xor
from typing import Any, Callable, Generator, NamedTuple, Optional
from urllib.parse import ParseResult, urlparse, urlunparse, parse_qs
import secrets

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.register_read_message import (
    ReadHoldingRegistersResponse,
    ReadWriteMultipleRegistersResponse,
)

import dali.gear
from dali import command, frame, gear, sequences
from dali.driver import trace_logging  # noqa: F401
from dali.device.helpers import DeviceInstanceTypeMapper
from dali.driver.serial import DriverSerialBase, DistributorQueue

_LOG = logging.getLogger("dali.driver")


class DriverDlc02Base:
    """
    Base level driver for the DLC-02 interface. Since the DLC-02 has two DALI buses
    attached, the 'send()' commands in this driver need to additionally specify which
    bus to use.

    Use the subclassed DriverDlc02 to create an instance which internally tracks which
    bus to use, and allows the use of 'send()' without needing to specify a bus.
    """

    def __init__(self, host: str):
        self.hostname = host

        # _modbus_client is set up when 'connect()' is run
        self._connected = asyncio.Event()
        self._modbus_client = AsyncModbusTcpClient(self.hostname)

        # Use a lock for all transmissions
        self._tx_lock = asyncio.Lock()

        # Log some information about the connection
        _LOG.info(f"Initialising dlc02 driver at 'tcp://{self.hostname}'")

    @staticmethod
    def _ints_to_words(in_ints: list[int]) -> list[int]:
        """
        Takes a list of single-byte values (i.e. ints less than 256) and
        converts them into a list of double-byte values, a.k.a. "words" in
        Modbus terminology.

        :param in_ints: A list of ints, in range(255)
        :return: A list of ints, in range(65535)
        """
        for in_int in in_ints:
            if in_int > 255:
                raise ValueError("A byte cannot be larger than 255")

        # If given an odd number of inputs, pad with 0 at the end
        if len(in_ints) % 2:
            in_ints.append(0)
        out_ints = [0] * (len(in_ints) >> 1)
        for out_idx in range(len(out_ints)):
            int_hi = int(in_ints[out_idx * 2])
            int_lo = int(in_ints[(out_idx * 2) + 1])
            out_ints[out_idx] = int_lo | (int_hi << 8)
        return out_ints

    @staticmethod
    def _words_to_ints(in_ints: list[int]) -> list[int]:
        """
        Takes a list of double-byte values (i.e. ints less than 65536), a.k.a.
        "words" in Modbus terminology, and converts them into a list of
        single-byte values.

        :param in_ints: A list of ints, in range(65535)
        :return: A list of ints, in range(255)
        """
        for in_int in in_ints:
            if in_int > 65535:
                raise ValueError("A word cannot be larger than 65535")

        out_ints = [0] * (len(in_ints) * 2)
        for idx in range(len(in_ints)):
            # The "high" 8 bits come first
            out_ints[idx * 2] = in_ints[idx] >> 8
            # The "low" 8 bits come second
            out_ints[(idx * 2) + 1] = in_ints[idx] & 0xFF

        return out_ints

    @property
    def is_connected(self) -> bool:
        """
        Flags whether the underlying transport is connected and ready for use

        :return: Boolean, true if connection is ready
        """
        return self._connected.is_set()

    async def wait_connected(self) -> None:
        """
        Blocks until the underlying transport is connected and ready for use

        :return: None
        """
        await self._connected.wait()

    async def connect(self) -> None:
        async with self._tx_lock:
            if self.is_connected:
                _LOG.info(
                    f"'connect()' called but Modbus client for {self.hostname} "
                    "already connected"
                )
                return

            # TODO: Add failure/retry handling
            # TODO: handle connection exceptions
            _LOG.info(f"Attempting to connect to DLC-02 at '{self.hostname}'")
            await self._modbus_client.connect()
            # TODO: Check the connection works

            self._connected.set()

    @staticmethod
    def _encode_dlc02_raw_dali_frame(
        dali_command: command.Command,
        bus: int,
        tx_serial: int,
    ) -> list[int]:
        if bus not in (1, 2):
            raise ValueError(f"DLC-02 'bus' must be either 1 or 2, not '{bus}'")
        if tx_serial > 255:
            raise ValueError(f"'tx_serial' must be less than 256")
        elif tx_serial < 0:
            raise ValueError(f"'tx_serial' must be at least 0")

        # Build the "control command byte"
        ctrl_cmd = 0
        if dali_command.sendtwice:
            # Bit 4: Tell DLC-02 to send command twice
            ctrl_cmd |= 0b00100000

        # Build the "command mode byte"
        if len(dali_command.frame) == 16:
            cmd_mode = 0
            hi_byte = 0
        elif len(dali_command.frame) == 24:
            cmd_mode = 1
            hi_byte = dali_command.frame[16:23]
        else:
            raise TypeError(
                f"DLC-02 only supports sending 16-bit or 24-bit DALI messages"
            )

        # Unpack the DALI frame
        md_byte = dali_command.frame[8:15]
        lo_byte = dali_command.frame[0:7]

        data_frame = DriverDlc02Base._ints_to_words(
            [
                0x12,  # Command header
                tx_serial,  # Request serial, returned in the response
                bus,  # DALI Bus
                ctrl_cmd,  # Control command byte
                cmd_mode,  # Command mode byte (2 byte command)
                hi_byte,  # DALI "High" byte
                md_byte,  # DALI "Mid" byte
                lo_byte,  # DALI "Low" byte
                0,  # DTR0
                0,  # DTR1
                0,  # DTR2
                1,  # Device Type, not used but needs to always be set
            ]
        )
        return data_frame

    class Dlc02DaliResponseStatus(Enum):
        NO = 0
        OK_8bit = 1
        OK_16bit = 2
        OK_24bit = 3
        ERROR_DALI_SHORT = 71
        ERROR_DALI_RX = 72

    @dataclass
    class Dlc02DaliResponse:
        rx_serial: int
        bus_id: int
        status: DriverDlc02Base.Dlc02DaliResponseStatus
        dali_ints: list[int] | None

    @staticmethod
    def _decode_dlc02_raw_dali_frame(
        dlc02_frame: list[int],
    ) -> DriverDlc02Base.Dlc02DaliResponse:
        dlc02_ints = DriverDlc02Base._words_to_ints(dlc02_frame)

        status_byte = dlc02_ints[3]
        if status_byte == 7:
            status_code = dlc02_ints[7]
            if status_code == 1:
                status = (
                    DriverDlc02Base.Dlc02DaliResponseStatus.ERROR_DALI_SHORT
                )
            elif status_code == 2:
                status = DriverDlc02Base.Dlc02DaliResponseStatus.ERROR_DALI_RX
            else:
                _LOG.error(
                    f"Invalid DLC-02 frame response status '{status_code}'"
                )
                status = DriverDlc02Base.Dlc02DaliResponseStatus.ERROR_DALI_RX
        else:
            status = DriverDlc02Base.Dlc02DaliResponseStatus(status_byte)

        dali_ints = None
        if status == DriverDlc02Base.Dlc02DaliResponseStatus.OK_8bit:
            dali_ints = [dlc02_ints[7]]
        elif status == DriverDlc02Base.Dlc02DaliResponseStatus.OK_16bit:
            dali_ints = [dlc02_ints[6], dlc02_ints[7]]
        elif status == DriverDlc02Base.Dlc02DaliResponseStatus.OK_24bit:
            dali_ints = [dlc02_ints[5], dlc02_ints[6], dlc02_ints[7]]

        return DriverDlc02Base.Dlc02DaliResponse(
            rx_serial=dlc02_ints[1],
            bus_id=dlc02_ints[2],
            status=status,
            dali_ints=dali_ints,
        )

    class Dlc02EventType(Enum):
        PUSHBUTTON = 1
        SLIDER = 2
        OCCUPANCY = 3
        LIGHT = 4

    class Dlc02EventPushbuttonInfo(Enum):
        RELEASED = 1
        PRESSED = 2
        SHORT_PRESS = 3
        DOUBLE_PRESS = 4
        LONG_PRESS_START = 5
        LONG_PRESS_REPEAT = 6
        LONG_PRESS_STOP = 7
        BUTTON_UNSTUCK = 8
        BUTTON_STUCK = 9

    class Dlc02EventOccupancyInfo(Enum):
        UNOCCUPIED_NO_MOVEMENT = 1
        OCCUPIED_NO_MOVEMENT = 2
        OCCUPIED_AND_MOVEMENT = 3

    @dataclass
    class Dlc02EventResponse:
        bus_id: int
        device_addr: int
        instance_num: int
        instance_type: DriverDlc02Base.Dlc02EventType
        event_info: int | DriverDlc02Base.Dlc02EventPushbuttonInfo | DriverDlc02Base.Dlc02EventOccupancyInfo

    @staticmethod
    def _decode_dlc02_event_frame(
        bus: int,
        dlc02_frame: list[int],
    ) -> DriverDlc02Base.Dlc02EventResponse:
        dlc02_ints = DriverDlc02Base._words_to_ints(dlc02_frame)

        device_addr = dlc02_ints[0]
        instance_num = dlc02_ints[1]
        instance_type = DriverDlc02Base.Dlc02EventType(dlc02_ints[2])
        event_info = dlc02_ints[3]
        if instance_type == DriverDlc02Base.Dlc02EventType.PUSHBUTTON:
            event_info = DriverDlc02Base.Dlc02EventPushbuttonInfo(event_info)
        elif instance_type == DriverDlc02Base.Dlc02EventType.OCCUPANCY:
            event_info = DriverDlc02Base.Dlc02EventOccupancyInfo(event_info)

        return DriverDlc02Base.Dlc02EventResponse(
            bus_id=0,
            device_addr=device_addr,
            instance_num=instance_num,
            instance_type=instance_type,
            event_info=event_info,
        )

    async def send(
        self,
        bus: int,
        msg: command.Command,
    ) -> Optional[command.Response]:
        async with self._tx_lock:
            # Only send if the driver is connected
            if not self.is_connected:
                _LOG.critical(f"DALI driver cannot send, not connected: {self}")
                raise IOError("DALI driver cannot send, not connected")

            response = None

            _LOG.debug(f"Sending DALI command: {msg}")
            # Pick a random number to use as the tx_serial
            tx_serial = secrets.randbelow(128)
            # Set the highest bit as 1 for bus 2, otherwise keep as 0 for bus 1
            if bus == 2:
                tx_serial |= 0b10000000
            dlc02_frame = self._encode_dlc02_raw_dali_frame(
                dali_command=msg, bus=bus, tx_serial=tx_serial
            )
            _LOG.debug(
                f"Modbus read/write register command to DLC-02: {dlc02_frame}"
            )
            result = await self._modbus_client.readwrite_registers(
                read_address=30001,
                read_count=4,
                write_address=40001,
                values=dlc02_frame,
                slave=0xFF,
            )
            if isinstance(result, ReadWriteMultipleRegistersResponse):
                dlc02_rsp = self._decode_dlc02_raw_dali_frame(result.registers)
                _LOG.debug(f"DLC-02 DALI command response: {dlc02_rsp}")
                if not dlc02_rsp.rx_serial == tx_serial:
                    err_msg = (
                        "Received response with invalid serial! Sent "
                        f"{tx_serial} but received {dlc02_rsp.rx_serial}"
                    )
                    _LOG.critical(err_msg)
                    raise IOError(err_msg)
                if not dlc02_rsp.bus_id == bus:
                    err_msg = (
                        "Received response with invalid DALI bus! Sent "
                        f"{bus} but received {dlc02_rsp.bus_id}"
                    )
                    _LOG.critical(err_msg)
                    raise IOError(err_msg)

                if msg.is_query:
                    # Default to assuming there is no response to the query
                    response = command.Response(None)
                    if dlc02_rsp.dali_ints:
                        if len(dlc02_rsp.dali_ints) == 1:
                            # Decode the response based on the type expected by
                            # the query
                            response = msg.response(
                                frame.BackwardFrame(dlc02_rsp.dali_ints[0])
                            )
                            _LOG.debug(f"DALI response received: {response}")
                        else:
                            _LOG.error(
                                f"DALI 8-bit response expected, but got: "
                                f"{dlc02_rsp.dali_ints}"
                            )

            return response

    async def listen_events(self):
        prev_event_idx = 0
        while True:
            # Check how many "Bus A" events there are to read out
            result = await self._modbus_client.read_holding_registers(
                address=34001,
                count=1,
                slave=0xFF,
            )
            if isinstance(result, ReadHoldingRegistersResponse):
                event_idx = result.registers[0] >> 8
                if event_idx != prev_event_idx:
                    new_events = event_idx - prev_event_idx
                    _LOG.info(f"New events: {new_events}")

                    # Read out the full list of events
                    results = []
                    dt_start = datetime.now()
                    for idx in range(prev_event_idx, event_idx):
                        result = (
                            await self._modbus_client.read_holding_registers(
                                address=34002 + idx * 2,
                                count=2,
                                slave=0xFF,
                            )
                        )
                        if isinstance(result, ReadHoldingRegistersResponse):
                            results.append(
                                self._decode_dlc02_event_frame(
                                    bus=1, dlc02_frame=result.registers
                                )
                            )
                        else:
                            _LOG.error(result)
                    dt_end = datetime.now()
                    elapse = int((dt_end - dt_start).microseconds / 1000)
                    prev_event_idx = event_idx
                    _LOG.info(
                        f"Got {len(results)} events in {elapse} ms.\n{results}"
                    )
            await asyncio.sleep(0.005)


class DriverDlc02(DriverSerialBase):
    uri_scheme = "dlc02"

    _host_clients: dict[str, DriverDlc02Base] = {}

    @classmethod
    def get_dlc02_base(cls, host: str) -> DriverDlc02Base:
        """
        get_dlc02_base() ensures that only one instance of a driver exists
        per Modbus host
        :param host: The host to create the client for, either IP address or hostname
        :return: An instance of DriverDlc02Base
        """
        if cls._host_clients.get(host, None) is None:
            cls._host_clients[host] = DriverDlc02Base(host)
        return cls._host_clients[host]

    def __init__(
        self,
        uri: str | ParseResult,
        dev_inst_map: Optional[DeviceInstanceTypeMapper] = None,
    ):
        super().__init__(uri=uri, dev_inst_map=dev_inst_map)

        # Get an instance of a DLC-02 driver for this bus
        self._dlc02_base = self.get_dlc02_base(host=self.uri.hostname)

        # Figure out which bus to use for this driver instance
        qs = parse_qs(self.uri.query)
        bus = qs.get("bus", None)
        if len(bus) != 1:
            raise ValueError(
                f"'bus' parameter must can only be given once; not {self.uri.query}"
            )
        bus = bus[0]
        if bus not in ("a", "A", "b", "B", "1", "2"):
            raise ValueError(
                f"'bus' parameter must be 'A', 'B', '1', or '2'; not {self.uri.query}"
            )
        if bus in ("a", "A"):
            self.dlc02_bus = 1
        elif bus in ("b", "B"):
            self.dlc02_bus = 2
        else:
            self.dlc02_bus = int(bus)

        # Note: this is only here for completeness, the DLC-02 does not support
        # observing raw commands from the DALI bus
        self._queue_rx_dali = DistributorQueue()

    async def connect(self, *, scan_dev_inst: bool = False) -> None:
        if self.is_connected:
            _LOG.warning(
                "'connect()' called but dlc02 driver for "
                f"'{self._dlc02_base.hostname}' already connected"
            )
            return

        await self._dlc02_base.connect()
        self._connected.set()

        # Scan the bus for control devices, and create a mapping of addresses
        # to instance types
        if scan_dev_inst:
            _LOG.info(
                f"DLC-02 {self.uri.hostname} bus {self.dlc02_bus}: "
                "Scanning DALI bus for control devices"
            )
            await self.run_sequence(self.dev_inst_map.autodiscover())
            _LOG.info(
                f"DLC-02 {self.uri.hostname} bus {self.dlc02_bus}: Found "
                f"{len(self.dev_inst_map.mapping)} enabled control "
                "device instances"
            )

    async def send(
        self, msg: command.Command, in_transaction: bool = False
    ) -> Optional[command.Response]:

        if not in_transaction:
            await self.transaction_lock.acquire()
        try:
            response = await self._dlc02_base.send(bus=self.dlc02_bus, msg=msg)
        finally:
            if not in_transaction:
                self.transaction_lock.release()

        return response

    def new_dali_rx_queue(self) -> DistributorQueue:
        _LOG.critical(
            "Listening to the DALI bus is not supported by the DLC-02, the "
            "queue from 'new_dali_rx_queue()' will always be empty!"
        )
        return DistributorQueue(self._queue_rx_dali)
