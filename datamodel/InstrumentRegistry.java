package com.satyam.trading2.datamodel;

import java.util.HashMap;
import java.util.Map;

public class InstrumentRegistry {

    public static final Map<Long, String> tokenToSymbol = new HashMap<>();


    /*
    * indigo
    * tmcv
    * bpcl
    * trent
    * asianpaint
    * bse
    * adanient
    * coforge
    * srf
    * M&MFIN
    * ajanatapharm
    * hpcl
    * AIIL
    * motilalofs
    * jkcement
    * ITCHOTELS
    * indusindbk
    * NAM-INDIA
    * unominda
    * GRSE
    * DRREDDY
    * MFSL
    * HINDCOPPER
    * WOCKPHARMA
    * FSL
    * HFCL
    * ECLERX
    * ANANTRAJ
    * HBLENGINE
    * FORCEMOT
    * COHANCE
    * POLYCAB
    * meesho
    * ENDURANCE
    * BAJAJ-AUTO
    * APARINDS
    * THERMAX
    * HEROMOTOCO
    * GVT&D
    * NYKAA
    * M&M
    * TIINDIA
    * EICHERMOT
    * LUPIN
    * ABCAPITAL
    * LAURUSLABS
    * BOSCHLTD
    * APLAPOLLO
    * HDFCLIFE
    * BHARATFORG
    * ABB
    *
    *
    *
    *
    * */

    static {
 // Automobile and Auto Components
    tokenToSymbol.put(884737L, "TMPV");
    tokenToSymbol.put(173057L, "EXIDEIND");
    tokenToSymbol.put(1076225L, "MOTHERSON");

    // Capital Goods
    tokenToSymbol.put(98049L, "BEL");
    tokenToSymbol.put(194561L, "CGPOWER");
    tokenToSymbol.put(54273L, "ASHOKLEY");

    // Chemicals
    tokenToSymbol.put(681985L, "PIDILITIND");
    tokenToSymbol.put(258049L, "FACT");

    // Construction and related materials
    tokenToSymbol.put(325121L, "AMBUJACEM");
    tokenToSymbol.put(2445313L, "RVNL");

    // Consumer Durables
    tokenToSymbol.put(2513665L, "HAVELLS");
    tokenToSymbol.put(103425L, "BERGEPAINT");

    // Consumer Services
    tokenToSymbol.put(1304833L, "ETERNAL");
    tokenToSymbol.put(387073L, "INDHOTEL");

    // Diversified
    tokenToSymbol.put(2796801L, "GODREJIND");

    // FMCG
    tokenToSymbol.put(424961L, "ITC");
    tokenToSymbol.put(4598529L, "NESTLEIND");
    tokenToSymbol.put(4843777L, "VBL");
    tokenToSymbol.put(878593L, "TATACONSUM");

    // Financial Services
    tokenToSymbol.put(779521L, "SBIN");
    tokenToSymbol.put(341249L, "HDFCBANK");
    tokenToSymbol.put(2426881L, "LICI");
    tokenToSymbol.put(4644609L, "JIOFIN");
    tokenToSymbol.put(1102337L, "SHRIRAMFIN");

    // Healthcare
    tokenToSymbol.put(177665L, "CIPLA");
    tokenToSymbol.put(5728513L, "MAXHEALTH");
    tokenToSymbol.put(2029825L, "ZYDUSLIFE");
    tokenToSymbol.put(70401L, "AUROPHARMA");

    // IT
    tokenToSymbol.put(1443L, "HCLTECH");
    tokenToSymbol.put(5195009L, "TATATECH");

    // METALS & MINING
    tokenToSymbol.put(3001089L, "JSWSTEEL");
    tokenToSymbol.put(364545L, "HINDZINC");
    tokenToSymbol.put(784129L, "VEDL");
    tokenToSymbol.put(895745L, "TATASTEEL");

    // Oil & Gas
    tokenToSymbol.put(738561L, "RELIANCE");
    tokenToSymbol.put(633601L, "ONGC");
    tokenToSymbol.put(5215745L, "COALINDIA");
    tokenToSymbol.put(415745L, "IOC");
    tokenToSymbol.put(359937L, "HINDPETRO");
    tokenToSymbol.put(2713345L, "GUJGASLTD");
    tokenToSymbol.put(2905857L, "PETRONET");
    tokenToSymbol.put(4488705L, "MGL");

    // Power & Utilities
    tokenToSymbol.put(2977281L, "NTPC");
    tokenToSymbol.put(3834113L, "POWERGRID");
    tokenToSymbol.put(4451329L, "ADANIPOWER");
    tokenToSymbol.put(912129L, "ADANIGREEN");
    tokenToSymbol.put(2615553L, "ADANIENSOL");
    tokenToSymbol.put(4574465L, "JSWENERGY");

    // REALTY
    tokenToSymbol.put(3771393L, "DLF");
    tokenToSymbol.put(824321L, "LODHA");

    // SERViCES
    tokenToSymbol.put(3463169L, "GMRAIRPORT");
    tokenToSymbol.put(4869121L, "JSWINFRA");
    tokenToSymbol.put(1215745L, "CONCOR");

    // TELECOM
    tokenToSymbol.put(7458561L, "INDUSTOWER");

    // TEXTILE
    tokenToSymbol.put(3817473L, "KPRMILL");
    tokenToSymbol.put(2479361L, "VTL");

    }

    public static String getSymbolFromToken(Long token) {
        return tokenToSymbol.get(token);
    }
}