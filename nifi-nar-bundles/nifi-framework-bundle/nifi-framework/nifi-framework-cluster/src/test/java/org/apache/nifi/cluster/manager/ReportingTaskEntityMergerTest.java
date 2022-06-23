package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.controller.status.RunStatus;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.status.ReportingTaskStatusDTO;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReportingTaskEntityMergerTest {

    @Test
    void TestMergeStatusFields() {
        final ReportingTaskEntity nodeOneReportingTaskEntity = getReportingTaskEntity("id1", RunStatus.Disabled.toString(), ValidationStatus.VALIDATING.toString());
        final ReportingTaskEntity nodeTwoReportingTaskEntity = getReportingTaskEntity("id2", ControllerServiceState.DISABLING.toString(), ValidationStatus.INVALID.toString());
        final Map<NodeIdentifier, ReportingTaskEntity> entityMap = new HashMap();
        entityMap.put(getNodeIdentifier("node1", 8000), nodeOneReportingTaskEntity);
        entityMap.put(getNodeIdentifier("node2", 8010), nodeTwoReportingTaskEntity);

        ReportingTaskEntityMerger merger = new ReportingTaskEntityMerger();
        merger.merge(nodeOneReportingTaskEntity, entityMap);
        assertEquals(2, nodeOneReportingTaskEntity.getStatus().getActiveThreadCount());
        assertEquals("DISABLING", nodeOneReportingTaskEntity.getStatus().getRunStatus());
    }

    private NodeIdentifier getNodeIdentifier(final String id, final int port) {
        return new NodeIdentifier(id, "localhost", port, "localhost", port+1, "localhost", port+2, port+3, true);
    }

    private ReportingTaskEntity getReportingTaskEntity(final String id, final String runStatus, final String validationStatus) {
        final ReportingTaskDTO dto = new ReportingTaskDTO();
        dto.setId(id);
        dto.setState(ScheduledState.STOPPED.name());
        dto.setValidationStatus(validationStatus);

        final ReportingTaskStatusDTO statusDto = new ReportingTaskStatusDTO();
        statusDto.setRunStatus(runStatus);
        statusDto.setActiveThreadCount(1);
        statusDto.setValidationStatus(validationStatus);

        final PermissionsDTO permissed = new PermissionsDTO();
        permissed.setCanRead(true);
        permissed.setCanWrite(true);

        final ReportingTaskEntity entity = new ReportingTaskEntity();
        entity.setComponent(dto);
        entity.setRevision(createNewRevision());
        entity.setDisconnectedNodeAcknowledged(true);
        entity.setPermissions(permissed);
        entity.setStatus(statusDto);

        return entity;
    }

    public RevisionDTO createNewRevision() {
        final RevisionDTO revisionDto = new RevisionDTO();
        revisionDto.setClientId(getClass().getName());
        revisionDto.setVersion(0L);
        return revisionDto;
    }
}