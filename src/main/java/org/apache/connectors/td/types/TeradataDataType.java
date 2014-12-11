package org.apache.connectors.td.types;

import java.sql.Types;

public enum TeradataDataType {

	BIGINT(Types.BIGINT, HiveDataType.INT),
	BINARY(Types.BINARY, HiveDataType.BINARY),
	BIT(Types.BIT, HiveDataType.BOOLEAN),
	BLOB(Types.BLOB, HiveDataType.BINARY),
	BOOLEAN(Types.BOOLEAN, HiveDataType.BOOLEAN),
	CHAR(Types.CHAR, HiveDataType.STRING), // TD Char not supported
	CLOB(Types.CLOB, HiveDataType.STRING),
	DATE(Types.DATE, HiveDataType.DATE),
	DECIMAL(Types.DECIMAL, HiveDataType.DECIMAL),
	DOUBLE(Types.DOUBLE, HiveDataType.DOUBLE),
	FLOAT(Types.FLOAT, HiveDataType.FLOAT),
	INTEGER(Types.INTEGER, HiveDataType.INT),
	LONGNVARCHAR(Types.LONGNVARCHAR, HiveDataType.STRING),
	LONGVARBINARY(Types.LONGVARBINARY, HiveDataType.BINARY),
	LONGVARCHAR(Types.LONGVARCHAR, HiveDataType.STRING),
	NUMERIC(Types.NUMERIC, HiveDataType.FLOAT),
	REAL(Types.REAL, HiveDataType.FLOAT),
	SMALLINT(Types.SMALLINT, HiveDataType.SMALLINT),
	TIMESTAMP(Types.TIMESTAMP, HiveDataType.TIMESTAMP),
	TINYINT(Types.TINYINT, HiveDataType.TINYINT),
	VARCHAR(Types.VARCHAR, HiveDataType.STRING), // TD varchar not supported
	VARBYTE(-3, HiveDataType.STRING); // TD varchar not supported
	
	private int typeID;
	private HiveDataType mappedType;
	
	TeradataDataType(int type, HiveDataType mappedType) {
		this.mappedType = mappedType;
		this.typeID = type;
	}
	
	public boolean matchesTypeID(int type) {
		return type == this.typeID;
	}
	
	public static TeradataDataType find(int typeID) {
		for (TeradataDataType t: TeradataDataType.values()) {
			if (t.matchesTypeID(typeID)) {
				return t;
			}
		}
		return null;
	}
	
	public HiveDataType getHiveDataType() {
		return this.mappedType;
	}
	
	
}
