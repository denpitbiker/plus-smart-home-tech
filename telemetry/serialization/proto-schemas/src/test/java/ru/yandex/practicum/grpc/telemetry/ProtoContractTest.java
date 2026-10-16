package ru.yandex.practicum.grpc.telemetry;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.TextFormat;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerOuterClass;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEvent;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEvent;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerProto;

import static org.junit.jupiter.api.Assertions.*;

class ProtoContractTest {

    static Stream<FileDescriptor> contracts() {
        return Stream.of(SensorEvent.getDescriptor(), HubEvent.getDescriptor(),
                CollectorControllerOuterClass.getDescriptor(), HubRouterControllerProto.getDescriptor());
    }

    @ParameterizedTest
    @MethodSource("contracts")
    void matchesBundledHubRouterDescriptor(FileDescriptor descriptor) throws Exception {
        String name = descriptor.getName().substring(descriptor.getName().lastIndexOf('/') + 1);
        try (var stream = getClass().getResourceAsStream("/hub-router-contract/" + name + ".txt")) {
            assertNotNull(stream, "Missing reference contract: " + name);
            var reference = FileDescriptorProto.newBuilder();
            TextFormat.merge(new InputStreamReader(stream, StandardCharsets.UTF_8), reference);
            assertEquals(reference.build(), descriptor.toProto());
        }
    }

    @Test
    void preservesOptionalZeroAndOneofFalseOnWire() throws Exception {
        var absentAction = DeviceActionProto.parseFrom(DeviceActionProto.getDefaultInstance().toByteArray());
        var zeroAction = DeviceActionProto.parseFrom(DeviceActionProto.newBuilder().setValue(0).build().toByteArray());
        assertFalse(absentAction.hasValue());
        assertTrue(zeroAction.hasValue());
        assertEquals(0, zeroAction.getValue());

        var absentCondition = ScenarioConditionProto.parseFrom(ScenarioConditionProto.getDefaultInstance().toByteArray());
        var falseCondition = ScenarioConditionProto.parseFrom(
                ScenarioConditionProto.newBuilder().setBoolValue(false).build().toByteArray());
        var zeroCondition = ScenarioConditionProto.parseFrom(
                ScenarioConditionProto.newBuilder().setIntValue(0).build().toByteArray());
        assertEquals(ScenarioConditionProto.ValueCase.VALUE_NOT_SET, absentCondition.getValueCase());
        assertEquals(ScenarioConditionProto.ValueCase.BOOL_VALUE, falseCondition.getValueCase());
        assertFalse(falseCondition.getBoolValue());
        assertEquals(ScenarioConditionProto.ValueCase.INT_VALUE, zeroCondition.getValueCase());
        assertEquals(0, zeroCondition.getIntValue());
    }

    @Test
    void generatesHubRouterUnaryRpc() {
        var method = HubRouterControllerGrpc.getHandleDeviceActionMethod();
        assertEquals("telemetry.service.hubrouter.HubRouterController/handleDeviceAction",
                method.getFullMethodName());
        assertEquals(io.grpc.MethodDescriptor.MethodType.UNARY, method.getType());
    }

    @Test
    void generatesExpectedUnaryRpcMethods() {
        assertEquals("telemetry.service.collector.CollectorController/CollectSensorEvent",
                CollectorControllerGrpc.getCollectSensorEventMethod().getFullMethodName());
        assertEquals("telemetry.service.collector.CollectorController/CollectHubEvent",
                CollectorControllerGrpc.getCollectHubEventMethod().getFullMethodName());
        assertEquals(io.grpc.MethodDescriptor.MethodType.UNARY,
                CollectorControllerGrpc.getCollectSensorEventMethod().getType());
        assertEquals(io.grpc.MethodDescriptor.MethodType.UNARY,
                CollectorControllerGrpc.getCollectHubEventMethod().getType());
    }
}
