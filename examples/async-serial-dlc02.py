"""
async-serial-luba.py - Example showing the usage of the LUBA driver

This example shows how to use python-dali with a Lunatone RS232 LUBA protocol
DALI interface. LUBA is a protocol defined by Lunatone for use in their
devices, they publish the specification on their website:
https://www.lunatone.com/wp-content/uploads/2021/04/LUBA_Protocol_EN.pdf

It is assumed that an appropriate RS232 interface device is connected between
the machine running python-dali and the Lunatone hardware, in writing this
example an FTDI USB interface was used but any other compatible device will
also work.

The example scans the DALI bus for any DALI 24-bit "control devices",
i.e. things like buttons, motion sensors, etc. (for now python-dali only
supports push buttons, other types will be discovered but their event
messages will be "unknown"); then for DALI 16-bit "control gear", i.e. lamps
etc. Various bits of information from the devices is printed out, showing how
to use things like the memory banks and a selected set of useful queries.



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
import asyncio
import logging

from dali.address import DeviceShort, GearBroadcast, GearShort, InstanceNumber
from dali.command import NumericResponse
from dali.device import light, occupancy, pushbutton
from dali.device.general import (
    DTR0,
    EventScheme,
    QueryEventSchemeResponse,
    UnknownEvent,
)
from dali.device.sequences import SetEventFilters, SetEventSchemes
from dali.driver.serial_dlc02 import DriverDlc02
from dali.exceptions import DALISequenceError
from dali.gear.colour import QueryColourValueDTR
from dali.gear.general import DAPC, QueryControlGearPresent
from dali.gear.sequences import QueryDT8ColourValue, SetDT8ColourValueTc
from dali.memory import info, location
from dali.sequences import QueryDeviceTypes, QueryGroups

# Define the path to the serial RS232 interface. The 'luba232' scheme tells
# the driver which protocol to use - so far only the LUBA protocol is
# implemented, but others may be added in the future
URI1 = "dlc02://192.168.10.10?bus=A"
URI2 = "dlc02://192.168.10.10?bus=B"


async def setup(uri:str):
    print(f"Setting up DriverDlc02 with uri '{uri}'")
    driver = DriverDlc02(uri=uri)
    await driver.connect(scan_dev_inst=True)
    return driver


async def query_bus(driver):
    # Test out DALI 24-bit control devices
    for dev_addr in {x[0] for x in driver.dev_inst_map.mapping.keys()}:
        print(f"\nReading memory banks for bus {driver.dlc02_bus} device A²{dev_addr}:")
        short = DeviceShort(dev_addr)
        try:
            dev_info = await driver.run_sequence(info.BANK_0.read_all(short))
            print(f"{dev_info}\n")
        except location.MemoryLocationNotImplemented:
            pass

    for dev_addr_inst, dev_type in driver.dev_inst_map.mapping.items():
        dev_addr = dev_addr_inst[0]
        inst_num = dev_addr_inst[1]
        print(
            f"\nEnabling device/instance scheme for bus {driver.dlc02_bus} A²{dev_addr}:I{inst_num} "
            f"(type {dev_type})"
        )
        sequence = SetEventSchemes(
            device=DeviceShort(dev_addr),
            instance=InstanceNumber(inst_num),
            scheme=EventScheme.device_instance,
        )
        rsp = await driver.run_sequence(sequence)
        if isinstance(rsp, QueryEventSchemeResponse):
            if rsp.value == EventScheme.device_instance:
                print("Success")
                continue
        print("Failed!")

    # Test out DALI 16-bit control gear
    print(f"\nSending DAPC(254) to bus {driver.dlc02_bus} broadcast address")
    await driver.send(DAPC(GearBroadcast(), 254))
    await asyncio.sleep(1.5)

    for ad in range(64):
        short = GearShort(ad)
        rsp = await driver.send(QueryControlGearPresent(short))
        if rsp:
            if rsp.value:
                print(f"\nFound control gear bus {driver.dlc02_bus} A{ad}")
                try:
                    dts = await driver.run_sequence(
                        QueryDeviceTypes(short), progress=print
                    )
                    print(f"  Device Types: {dts}")
                    # Use some DT8 commands to play with colour temperature
                    if 8 in dts:
                        tc = await driver.run_sequence(
                            QueryDT8ColourValue(
                                address=short,
                                query=QueryColourValueDTR.ColourTemperatureTC,
                            )
                        )
                        print(f"  Tc for A{ad}: {tc}")
                        if isinstance(tc, int):
                            if tc < 360:
                                tc += 50
                            else:
                                tc = 100
                            print(f"  Setting Tc for A{ad} to {tc} Mired")
                            await driver.run_sequence(
                                SetDT8ColourValueTc(address=short, tc_mired=tc)
                            )
                except DALISequenceError:
                    pass
                try:
                    gps = await driver.run_sequence(
                        QueryGroups(short), progress=print
                    )
                    print(
                        f"  Group Memberships: {list(gps) if gps else 'None'}"
                    )
                except DALISequenceError:
                    pass

    await asyncio.sleep(1.5)
    print(f"\nSending DAPC(0) to bus {driver.dlc02_bus} broadcast address\n")
    await driver.send(DAPC(GearBroadcast(), 0))


async def main():
    driver1, driver2 = await asyncio.gather(setup(URI1), setup(URI2))
    await asyncio.gather(query_bus(driver1), query_bus(driver2))
    await driver1._dlc02_base.listen_events()


if __name__ == "__main__":
    logger = logging.getLogger("dali")
    log_handler = logging.StreamHandler()
    log_formatter = logging.Formatter("%(asctime)s %(levelname)s:%(message)s")
    log_formatter.default_msec_format = "%s.%03d"
    log_handler.setFormatter(log_formatter)
    logger.addHandler(log_handler)
    logger.setLevel("INFO")
    # logging.getLogger("dali.driver").setLevel("DEBUG")

    asyncio.run(main())
