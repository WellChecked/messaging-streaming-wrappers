# topic structure = namespace/group_id/message_type/edge_node_id/[device_id]
from collections import OrderedDict
from enum import IntEnum
from functools import cache

import sparkplub_b_packets.core.sparkplug_b_pb2 as sparkplug_b_pb2

from sparkplub_b_packets.builder import EdgeDevice, EdgeNode
from sparkplub_b_packets.core.sparkplug_b import MetricDataType, ParameterDataType
from sparkplub_b_packets.packets import DBirthPacket, DDataPacket, Metric, MetricProperty, NBirthPacket, NDataPacket


# message types:
# NBIRTH – Birth certificate for Sparkplug Edge Nodes
# NDEATH – Death certificate for Sparkplug Edge Nodes
# DBIRTH – Birth certificate for Devices
# DDEATH – Death certificate for Devices
# NDATA – Edge Node data message
# DDATA – Device data message
# NCMD – Edge Node command message
# DCMD – Device command message
# STATE – Sparkplug Host Application state message

class AliasMap(IntEnum):
    Next_Server = 0
    Rebirth = 1
    Reboot = 2
    CPU_Load = 3
    Memory_Avail = 4
    Node_Metric2 = 5
    Node_Metric3 = 6
    OGI_Setpoint = 7
    OGI_Flow = 8
    OGI_Count = 9
    OGI_Uptime = 10
    OGI_Confidence = 11


@cache
def get_nvidia_metrics():
    metrics = OrderedDict()
    metrics["Node Control/Next Server"] = Metric(
        alias="Node Control/Next Server",
        alias_map=AliasMap.Next_Server,
        data_type=MetricDataType.Boolean,
        value=False
    )
    metrics["Node Control/Rebirth"] = Metric(
        alias="Node Control/Rebirth",
        alias_map=AliasMap.Rebirth,
        data_type=MetricDataType.Boolean,
        value=False
    )
    metrics["Node Control/Reboot"] = Metric(
        alias="Node Control/Reboot",
        alias_map=AliasMap.Reboot,
        data_type=MetricDataType.Boolean,
        value=False
    )
    metrics["CPU/Load"] = Metric(
        alias="CPU/Load",
        alias_map=AliasMap.CPU_Load,
        data_type=MetricDataType.Float,
        value=0.0
    )
    metrics["Memory/Avail"] = Metric(
        alias="Memory/Avail",
        alias_map=AliasMap.Memory_Avail,
        data_type=MetricDataType.UInt64,
        value=0
    )
    return metrics


@cache
def get_ogi_metrics():
    metrics = OrderedDict()
    metrics["OGI/Flow"] = Metric(
        alias="OGI/Flow",
        alias_map=AliasMap.OGI_Flow,
        data_type=MetricDataType.Float,
        value=0.0,
        properties=[
            MetricProperty(
                name="Setpoint",
                data_type=ParameterDataType.UInt8,
                value=0
            ),
            MetricProperty(
                name="Quality",
                data_type=ParameterDataType.Float,
                value=0.0
            ),
            MetricProperty(
                name="Count",
                data_type=ParameterDataType.UInt32,
                value=0
            )
        ]
    )
    metrics["OGI/Uptime"] = Metric(
        alias="OGI/Uptime",
        alias_map=AliasMap.OGI_Uptime,
        data_type=MetricDataType.Float,
        value=0.0
    )
    return metrics


class NvidiaNBirthPacket(NBirthPacket):

    def __init__(self, group: str, node: str):
        super().__init__(group=group, node=node, metrics=get_nvidia_metrics())


class NvidiaNDataPacket(NDataPacket):

    def __init__(self, group: str, node: str):
        super().__init__(group=group, node=node, metrics=get_nvidia_metrics())


class OGIDBirthPacket(DBirthPacket):

    def __init__(self, group: str, node: str, device_id: str = "OGI"):
        super().__init__(group=group, node=node, device_id=device_id, metrics=get_ogi_metrics())


class OGIDDataPacket(DDataPacket):

    def __init__(self, group: str, node: str, device_id: str = "OGI"):
        super().__init__(group=group, node=node, device_id=device_id, metrics=get_ogi_metrics())


class EdgeComputer(EdgeNode):

    def birth_certificate(self) -> NBirthPacket:
        packet = NvidiaNBirthPacket(group=self._group, node=self._node)
        return packet

    def data_packet(self, cpu_load: float, memory_avail: int) -> NDataPacket:
        packet = NvidiaNDataPacket(group=self._group, node=self._node)
        packet.metrics["CPU/Load"].value = cpu_load
        packet.metrics["Memory/Avail"].value = memory_avail
        return packet


class OGIDevice(EdgeDevice):

    def __init__(self, group: str, node: str, device_id: str = "OGI"):
        super().__init__(group, node, device_id)

    def birth_certificate(self) -> DBirthPacket:
        packet = OGIDBirthPacket(group=self.group, node=self.node, device_id=self.device)
        return packet

    def data_packet(self, setpoint: int, flow: float, quality: float, count: int, uptime: float) -> DDataPacket:
        packet = OGIDDataPacket(group=self.group, node=self.node, device_id=self.device)
        packet.metrics["OGI/Flow"].value = flow
        for prop in packet.metrics["OGI/Flow"].properties:
            if prop.name == "Setpoint":
                prop.value = setpoint
            elif prop.name == "Quality":
                prop.value = quality
            elif prop.name == "Count":
                prop.value = count
        packet.metrics["OGI/Uptime"].value = uptime
        return packet


if __name__ == '__main__':
    def print_metrics(packet):
        payload = sparkplug_b_pb2.Payload()
        payload.ParseFromString(packet)
        for metric in payload.metrics:
            print(metric)

    edge_computer = EdgeComputer(group='tenant.location', node='edge')

    nbirth_packet = edge_computer.birth_certificate()
    nbirth_payload = nbirth_packet.payload()
    print(f"Topic: {nbirth_packet.topic}\nPayload Metrics:")
    print_metrics(nbirth_payload)

    ndata_packet = edge_computer.data_packet(cpu_load=1.23, memory_avail=123412341234)
    ndata_payload = ndata_packet.payload()
    print(f"Topic: {ndata_packet.topic}\nPayload Metrics:")
    print_metrics(ndata_payload)

    ndeath_packet = edge_computer.death_certificate()
    ndeath_payload = ndeath_packet.payload()
    print(f"Topic: {ndeath_packet.topic}\nPayload Metrics:")
    print_metrics(ndeath_payload)

    ogi_device = OGIDevice(group='tenant.location', node='edge', device_id='OGI')

    dbirth_packet = ogi_device.birth_certificate()
    dbirth_payload = dbirth_packet.payload()
    print(f"Topic: {dbirth_packet.topic}\nPayload Metrics:")
    print_metrics(dbirth_payload)

    ddata_packet = ogi_device.data_packet(setpoint=1, flow=92.23, quality=98.9, count=1600, uptime=100.0)
    ddata_payload = ddata_packet.payload()
    print(f"Topic: {ddata_packet.topic}\nPayload Metrics:")
    print_metrics(ddata_payload)

    ddeath_packet = ogi_device.death_certificate()
    ddeath_payload = ddeath_packet.payload()
    print(f"Topic: {ddeath_packet.topic}\nPayload Metrics:")
    print_metrics(ddeath_payload)
