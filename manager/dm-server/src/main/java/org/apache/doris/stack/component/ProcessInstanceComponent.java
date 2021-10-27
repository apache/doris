package org.apache.doris.stack.component;

import lombok.extern.slf4j.Slf4j;
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
        if (processEntity != null) {
            throw new ServerException("You already have an installation in the current environment!");
        }
        return processInstanceRepository.save(processInstance).getId();
    }

    /**
     * save process and return id
     */
    public ProcessInstanceEntity queryProcessByuserId(int userId) {
        return processInstanceRepository.queryProcessByuserId(userId);
    }

    /**
     * save process and return id
     */
    public ProcessInstanceEntity queryProcessById(int processId) {
        Optional<ProcessInstanceEntity> optional = processInstanceRepository.findById(processId);
        if (!optional.isPresent()) {
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
