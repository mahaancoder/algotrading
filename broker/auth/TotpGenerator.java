package com.satyam.trading2.broker.auth;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Minimal RFC 6238 TOTP generator for INDstocks token generation.
 *
 * INDstocks stores the TOTP secret as a base32 string (the same format
 * Google Authenticator displays). We decode it, HMAC-SHA1 the current
 * 30-second time step, and return the 6-digit code.
 *
 * No external dependencies — uses only javax.crypto and java.util.Base64.
 * Base32 decoding is implemented inline since java.util.Base64 doesn't
 * handle base32.
 */
public final class TotpGenerator {

    private static final int TIME_STEP_SECONDS = 30;
    private static final int CODE_DIGITS = 6;
    private static final String HMAC_ALGORITHM = "HmacSHA1";

    private TotpGenerator() {}

    /**
     * Generate a TOTP code from a base32-encoded secret.
     * @param base32Secret the shared secret as shown by INDstocks TOTP setup
     * @return 6-digit zero-padded code, e.g. "123456"
     */
    public static String generateCode(String base32Secret) {
        byte[] key = base32Decode(base32Secret);
        long timeStep = System.currentTimeMillis() / 1000 / TIME_STEP_SECONDS;
        return generateCode(key, timeStep);
    }

    private static String generateCode(byte[] key, long timeStep) {
        byte[] timeBytes = ByteBuffer.allocate(8).putLong(timeStep).array();
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(key, HMAC_ALGORITHM));
            byte[] hash = mac.doFinal(timeBytes);

            // Dynamic truncation per RFC 4226
            int offset = hash[hash.length - 1] & 0xF;
            int binary = ((hash[offset] & 0x7F) << 24)
                    | ((hash[offset + 1] & 0xFF) << 16)
                    | ((hash[offset + 2] & 0xFF) << 8)
                    | (hash[offset + 3] & 0xFF);

            int otp = binary % (int) Math.pow(10, CODE_DIGITS);
            return String.format("%0" + CODE_DIGITS + "d", otp);
        } catch (Exception e) {
            throw new RuntimeException("TOTP generation failed", e);
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Base32 decoder (RFC 4648) — no padding required
    // ─────────────────────────────────────────────────────────────────────

    private static final String BASE32_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

    static byte[] base32Decode(String encoded) {
        if (encoded == null) throw new IllegalArgumentException("base32 secret is null");
        // Remove spaces, uppercase, strip padding
        String clean = encoded.trim().toUpperCase().replace(" ", "").replaceAll("=+$", "");
        StringBuilder bits = new StringBuilder();
        for (char c : clean.toCharArray()) {
            int idx = BASE32_CHARS.indexOf(c);
            if (idx < 0) throw new IllegalArgumentException("Invalid base32 char: " + c);
            bits.append(String.format("%5s", Integer.toBinaryString(idx)).replace(' ', '0'));
        }
        // Group into bytes (8 bits each); drop trailing partial byte
        int byteCount = bits.length() / 8;
        byte[] result = new byte[byteCount];
        for (int i = 0; i < byteCount; i++) {
            result[i] = (byte) Integer.parseInt(bits.substring(i * 8, i * 8 + 8), 2);
        }
        return result;
    }
}
