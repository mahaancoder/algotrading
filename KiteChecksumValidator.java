package com.satyam.trading2.order;


import com.satyam.trading2.datamodel.KiteWebhookPayload;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KiteChecksumValidator {

    @Value("${kite.api.secret}")
    private String apiSecret;

    public boolean isValid(KiteWebhookPayload payload) {
        try{
            String checkSumData = payload.getOrderId() +  payload.getOrderTimestamp() + apiSecret;
            String generatedCheckSum =  DigestUtils.sha256Hex(checkSumData);
            return generatedCheckSum.equals(payload.getChecksum());
        }
        catch (Exception e){
            return false;
        }
    }
}
