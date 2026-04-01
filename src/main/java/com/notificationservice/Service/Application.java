package com.notificationservice.Service;

import com.notificationservice.Config.AppConstants;
import com.notificationservice.dto.EmailRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
public class Application {

    @Autowired
    private JavaMailSender javaMailSender;

        @KafkaListener(topics = AppConstants.TOPIC, groupId = "group_Email")
        public void consumeMessage(String emailRequest) {
            ObjectMapper mapper = new ObjectMapper();
            EmailRequest request = mapper.readValue(emailRequest,EmailRequest.class);
            SimpleMailMessage sm = new SimpleMailMessage();
            sm.setTo(request.getTo());
            sm.setSubject(request.getSubject());
            sm.setText(request.getBody());

            javaMailSender.send(sm);

        }


}
