package com.satyam.trading2.domain.service;

import com.satyam.trading2.datamodel.Instrument;
import com.satyam.trading2.datamodel.OrderRequest;
import com.satyam.trading2.datamodel.TradeSide;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Objects;

import static com.satyam.trading2.datamodel.Nifty500Stocks.Nifty500SymbolToInstrument;

@Service
@RequiredArgsConstructor
public class DefaultOrderSanitizer implements OrderSanitizer {

    @Override
    public OrderRequest sanitize(OrderRequest request) {
        Instrument instrument = Nifty500SymbolToInstrument.get(request.getSymbol());
//        sanitizeQuantity(request, instrument);
        sanitizePrice(request, instrument);
        validateOrder(request);
        return request;
    }

    private void sanitizeQuantity(OrderRequest request, Instrument instrument) {
        return;
/*        int lotSize = instrument.getLotSize();
//        if (lotSize > 1) {
//            int qty = request.getQuantity();
//            if (qty % lotSize != 0) {
//                int correctedQty = (qty / lotSize) * lotSize;
//                request.setQuantity(correctedQty);
//            }
   }
 */
    }

    private void sanitizePrice(OrderRequest request, Instrument instrument) {
        if (Objects.equals(request.getOrderType(), "MARKET")) return;
        double tick = instrument.getTickSize();
        double rawPrice = request.getPrice();
        double finalPrice;
        if (request.getSide() == TradeSide.BUY) {
            finalPrice = Math.floor(rawPrice / tick) * tick;
        } else {
            finalPrice = Math.ceil(rawPrice / tick) * tick;
        }
        request.setPrice(finalPrice);
    }

    private void validateOrder(OrderRequest request) {
        if (request.getQuantity() <= 0) throw new IllegalArgumentException("Invalid quantity");
        if (Objects.equals(request.getOrderType(), "LIMIT") && request.getPrice() <= 0) throw new IllegalArgumentException("Invalid price");
//        if ((Objects.equals(request.getOrderType(), "SL") || Objects.equals(request.getOrderType(), "SL_M")) && request.getTriggerPrice() == null) {
//            throw new IllegalArgumentException("Trigger price required");
//        }
    }
}