package ru.yandex.practicum.analyzer.grpc;

import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.analyzer.analysis.DeviceCommand;
import ru.yandex.practicum.analyzer.config.HubRouterSettings;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc.HubRouterControllerBlockingStub;

@Service
public class HubRouterClient {
    private static final Logger log = LoggerFactory.getLogger(HubRouterClient.class);
    private final HubRouterSettings settings;

    @GrpcClient("hub-router")
    private HubRouterControllerBlockingStub stub;

    public HubRouterClient(HubRouterSettings settings) {
        this.settings = settings;
    }

    public void send(List<DeviceCommand> commands) {
        for (var command : commands) {
            var action = DeviceActionProto.newBuilder().setSensorId(command.sensorId()).setType(switch (command.type()) {
                case ACTIVATE -> ActionTypeProto.ACTIVATE;
                case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
                case INVERSE -> ActionTypeProto.INVERSE;
                case SET_VALUE -> ActionTypeProto.SET_VALUE;
            });
            if (command.value() != null) {
                action.setValue(command.value());
            }
            var request = DeviceActionRequest.newBuilder()
                    .setHubId(command.hubId()).setScenarioName(command.scenarioName()).setAction(action)
                    .setTimestamp(Timestamp.newBuilder().setSeconds(command.timestamp().getEpochSecond())
                            .setNanos(command.timestamp().getNano())).build();
            try {
                stub.withDeadlineAfter(settings.deadline().toNanos(), TimeUnit.NANOSECONDS).handleDeviceAction(request);
            } catch (RuntimeException e) {
                log.error("Command failed hub={} scenario={} sensor={} action={}",
                        command.hubId(), command.scenarioName(), command.sensorId(), command.type(), e);
                throw e;
            }
        }
    }
}
