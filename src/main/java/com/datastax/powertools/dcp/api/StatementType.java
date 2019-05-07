package com.datastax.powertools.dcp.api;

/*
 *
 * @author Sebastián Estévez on 5/1/19.
 *
 */


public enum StatementType {
    PutItem,
    GetItem,
    CreateTable,
    DeleteItem,
    Query;

    public static StatementType valueOfLowerCase(String arg){
        StatementType[] stmtValues = StatementType.values();

        for (StatementType stmtValue : stmtValues) {
            if(stmtValue.toString().toLowerCase().equals(arg.toLowerCase())){
                return stmtValue;
            }
        }
        throw new RuntimeException("invalid argument for Statement Type");
    }
}

