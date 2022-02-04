package com.dieselpoint.norm.sqlmakers;

import javax.persistence.Column;
import java.math.BigDecimal;

public class PostgresMaker extends StandardSqlMaker {
	protected String getColType(Class<?> dataType, int length, int precision, int scale) {
		String colType;

		if (dataType.equals(Integer.class) || dataType.equals(int.class)) {
			colType = "integer";
		} else if (dataType.equals(Long.class) || dataType.equals(long.class)) {
			colType = "bigint";
		} else if (dataType.equals(Double.class) || dataType.equals(double.class)) {
			colType = "double";
		} else if (dataType.equals(Float.class) || dataType.equals(float.class)) {
			colType = "float";
		} else if (dataType.equals( BigDecimal.class)) {
			colType = "decimal(" + precision + "," + scale + ")";
		} else if (dataType.equals(java.util.Date.class)) {
			colType = "datetime";
		} else if (dataType.equals(java.util.UUID.class)) {
			colType = "uuid";
		} else if (dataType.equals(java.sql.Timestamp.class)) {
			colType = "timestamp";
		} else {
			colType = "varchar(" + length + ")";
		}
		return colType;
	}

	@Override
	public String getCreateTableSql(Class<?> clazz) {
		StringBuilder buf = new StringBuilder();
		StandardPojoInfo pojoInfo = getPojoInfo( clazz);
		buf.append("create table ");
		buf.append(pojoInfo.table);
		buf.append(" (");
		boolean needsComma = false;
		for (Property prop : pojoInfo.propertyMap.values()) {

			if (needsComma) {
				buf.append(',');
			}
			needsComma = true;

			Column columnAnnot = prop.columnAnnotation;
			if (columnAnnot == null) {

				buf.append(prop.name);
				buf.append(" ");
				if (prop.isGenerated) {
					if (prop.dataType.equals( java.util.UUID.class )) {
						buf.append( " uuid default gen_random_uuid()");
					}
					else {
						buf.append( " serial" );
					}
				} else {
					buf.append(getColType(prop.dataType, 255, 10, 2));
				}

			} else {
				if (columnAnnot.columnDefinition() == null) {

					// let the column def override everything
					buf.append(columnAnnot.columnDefinition());

				} else {

					buf.append(prop.name);
					buf.append(" ");
					if (prop.isGenerated) {
						buf.append(" serial");
					} else {
						buf.append(getColType(prop.dataType, columnAnnot.length(), columnAnnot.precision(), columnAnnot.scale()));
					}

					if (columnAnnot.unique()) {
						buf.append(" unique");
					}

					if (!columnAnnot.nullable()) {
						buf.append(" not null");
					}
				}
			}
		}

		if (pojoInfo.primaryKeyName != null) {
			buf.append(", primary key (");
			buf.append(pojoInfo.primaryKeyName);
			buf.append(")");
		}

		buf.append(")");

		return buf.toString();
	}


}
