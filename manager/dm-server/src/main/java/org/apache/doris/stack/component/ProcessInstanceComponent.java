package org.apache.doris.stack.component;

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.stack.constants.ProcessTypeEnum;
import org.apache.doris.stack.dao.ProcessInstanceRepository;
import org.apache.doris.stack.entity.ProcessInstanceEntity;
import org.apache.doris.stack.exceptions.ServerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@Slf4j
public class ProcessInstanceComponent {

    @Autowired
    private ProcessInstanceRepository processInstanceRepository;

    /**
     * save process and return id
     */
    public int saveProcessInstance(ProcessInstanceEntity processInstance) {
        //query whether there is a process currently being installed
        ProcessInstanceEntity processEntity = processInstanceRepository.queryProcessByuserId(processInstance.getUserId());
        if (processEntity != null && processEntity.getId() != processInstance.getId()) {
            throw new ServerException("You already have an installation in the current environment!");
        }
        return processInstanceRepository.save(processInstance).getId();
    }

    public int refreshProcess(int processId, int clusterId, int userId, ProcessTypeEnum processType) {
        ProcessInstanceEntity processInstance = queryProcessById(processId);
        if (processInstance == null) {
            processInstance = new ProcessInstanceEntity(clusterId, userId, processType);
        } else {
            processInstance.setProcessType(processType);
        }
        return processInstanceRepository.save(processInstance).getId();
    }

    public ProcessInstanceEntity queryProcessByuserId(int userId) {
        return processInstanceRepository.queryProcessByuserId(userId);
    }

    /**
     * query process by id
     */
    public ProcessInstanceEntity queryProcessById(int processId) {
        Optional<ProcessInstanceEntity> optional = processInstanceRepository.findById(processId);
        if (optional.isPresent()) {
            return optional.get();
        }
        return null;
    }

    /**
     * update process status
     */
    public void updateProcess(ProcessInstanceEntity processInstance) {
        processInstanceRepository.save(processInstance);
    }
}
