package com.data.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasTypeUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;



/**
 * @program: atlas-learn
 * @author: huzekang
 * @create: 2021-11-10 16:45
 **/
public class DorisApp {
	public static final String ENTITY_TYPE_DATASET = "DataSet";

	public static final String ENUM_DEF_DORIS_TABLE_TYPE = "Doris_tableType_V2";
	public static final String ENTITY_DEF_DORIS_DB = "Doris_db_v2";
	public static final String ENTITY_DEF_DORIS_TABLE = "Doris_table_v2";
	public static final String ENTITY_DEF_DORIS_COLUMN = "Doris_column_v2";
	public static final String RELAT_DEF_DORIS_DB_TABLES = "Doris_db_tables_v2";
	public static final String RELAT_DEF_DORIS_TB_COLUMNS = "Doris_tb_columns_v2";

	public static void main(String[] args) throws AtlasServiceException {
		String[] atlasServerUrls = new String[]{"http://10.211.55.8:21000/"};
		String[] basicAuthUsernamePassword = new String[]{"admin", "admin"};
		AtlasClientV2 atlasClientV2 = new AtlasClientV2(atlasServerUrls, basicAuthUsernamePassword);
		//createTypes(atlasClientV2);
		deleteTypes(Lists.newArrayList(
				RELAT_DEF_DORIS_DB_TABLES,
				RELAT_DEF_DORIS_TB_COLUMNS,
				ENTITY_DEF_DORIS_DB,
				ENTITY_DEF_DORIS_TABLE,
				ENTITY_DEF_DORIS_COLUMN,
				ENUM_DEF_DORIS_TABLE_TYPE
				),atlasClientV2);
	}

	private static void createTypes(AtlasClientV2 atlasClientV2) throws AtlasServiceException {
		final List<AtlasStructDef.AtlasAttributeDef> dbAtlasAttributeDefs = Arrays.asList(
				AtlasTypeUtil.createOptionalAttrDef("create_time", "date")
		);
		final List<AtlasStructDef.AtlasAttributeDef> tbAtlasAttributeDefs = Arrays.asList(
				AtlasTypeUtil.createOptionalAttrDef("size", "long"),
				AtlasTypeUtil.createOptionalAttrDef("replicaCount", "int"),
				AtlasTypeUtil.createOptionalAttrDef("create_time", "date"),
				AtlasTypeUtil.createOptionalAttrDef("modelType", ENUM_DEF_DORIS_TABLE_TYPE),
				AtlasTypeUtil.createOptionalAttrDef("DUPLICATE_KEY", "string"),
				AtlasTypeUtil.createOptionalAttrDef("AGGREGATE_KEY", "string"),
				AtlasTypeUtil.createOptionalAttrDef("UNIQUE_KEY", "string"),
				AtlasTypeUtil.createOptionalAttrDef("Engine", "string"),
				AtlasTypeUtil.createOptionalAttrDef("comment", "string")
		);
		final AtlasEntityDef dorisDbEntityDef = new AtlasEntityDef(ENTITY_DEF_DORIS_DB, null, "1.0", dbAtlasAttributeDefs, Sets.newHashSet(ENTITY_TYPE_DATASET));
		final AtlasEntityDef dorisTableEntityDef = new AtlasEntityDef(ENTITY_DEF_DORIS_TABLE, null, "1.0", tbAtlasAttributeDefs, Sets.newHashSet(ENTITY_TYPE_DATASET));
		final AtlasEntityDef dorisColumnEntityDef = AtlasTypeUtil.createClassTypeDef(ENTITY_DEF_DORIS_COLUMN,
				Collections.singleton(ENTITY_TYPE_DATASET),
				AtlasTypeUtil.createOptionalAttrDef("isNull", "boolean"),
				AtlasTypeUtil.createOptionalAttrDef("dataType", "string"),
				AtlasTypeUtil.createOptionalAttrDef("comment", "string")
		);
		final AtlasEnumDef dorisTableTypeEnumDef = new AtlasEnumDef(ENUM_DEF_DORIS_TABLE_TYPE, null,
				Arrays.asList(
						new AtlasEnumDef.AtlasEnumElementDef("Duplicate", null, 1),
						new AtlasEnumDef.AtlasEnumElementDef("Unique", null, 2),
						new AtlasEnumDef.AtlasEnumElementDef("Aggregate", null, 3)
				)
		);

		final AtlasRelationshipDef dbTablesDef = new AtlasRelationshipDef(RELAT_DEF_DORIS_DB_TABLES, null, "1.0",
				AtlasRelationshipDef.RelationshipCategory.AGGREGATION, AtlasRelationshipDef.PropagateTags.NONE,
				new AtlasRelationshipEndDef(ENTITY_DEF_DORIS_TABLE, "db", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, false),
				AtlasTypeUtil.createRelationshipEndDef(ENTITY_DEF_DORIS_DB, "tables", AtlasStructDef.AtlasAttributeDef.Cardinality.SET, true));

		final AtlasRelationshipDef tableColumnsDef = new AtlasRelationshipDef(RELAT_DEF_DORIS_TB_COLUMNS, null, "1.0",
				AtlasRelationshipDef.RelationshipCategory.COMPOSITION, AtlasRelationshipDef.PropagateTags.NONE,
				new AtlasRelationshipEndDef(ENTITY_DEF_DORIS_TABLE, "columns", AtlasStructDef.AtlasAttributeDef.Cardinality.SET, true),
				AtlasTypeUtil.createRelationshipEndDef(ENTITY_DEF_DORIS_COLUMN, "table", AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, false));
		// 批量创建type
		final AtlasTypesDef atlasTypesDef = new AtlasTypesDef(Lists.newArrayList(dorisTableTypeEnumDef), Lists.newArrayList(), Lists.newArrayList(),
				Lists.newArrayList(dorisDbEntityDef, dorisTableEntityDef, dorisColumnEntityDef),
				Lists.newArrayList(dbTablesDef, tableColumnsDef)
		);
		batchCreateTypes(atlasClientV2, atlasTypesDef);
	}

	public static void log(String message) {
		System.out.println("[" + new Date() + "] " + message);
	}

	private static AtlasTypesDef batchCreateTypes(AtlasClientV2 client, AtlasTypesDef typesDef) throws AtlasServiceException {
		AtlasTypesDef typesToCreate = new AtlasTypesDef();

		for (AtlasEnumDef enumDef : typesDef.getEnumDefs()) {
			if (client.typeWithNameExists(enumDef.getName())) {
				DorisApp.log(enumDef.getName() + ": type already exists. Skipping");
			} else {
				typesToCreate.getEnumDefs().add(enumDef);
			}
		}

		for (AtlasStructDef structDef : typesDef.getStructDefs()) {
			if (client.typeWithNameExists(structDef.getName())) {
				DorisApp.log(structDef.getName() + ": type already exists. Skipping");
			} else {
				typesToCreate.getStructDefs().add(structDef);
			}
		}

		for (AtlasEntityDef entityDef : typesDef.getEntityDefs()) {
			if (client.typeWithNameExists(entityDef.getName())) {
				DorisApp.log(entityDef.getName() + ": type already exists. Skipping");
			} else {
				typesToCreate.getEntityDefs().add(entityDef);
			}
		}

		for (AtlasClassificationDef classificationDef : typesDef.getClassificationDefs()) {
			if (client.typeWithNameExists(classificationDef.getName())) {
				DorisApp.log(classificationDef.getName() + ": type already exists. Skipping");
			} else {
				typesToCreate.getClassificationDefs().add(classificationDef);
			}
		}

		for (AtlasRelationshipDef relationshipDef : typesDef.getRelationshipDefs()) {
			if (client.typeWithNameExists(relationshipDef.getName())) {
				DorisApp.log(relationshipDef.getName() + ": type already exists. Skipping");
			} else {
				typesToCreate.getRelationshipDefs().add(relationshipDef);
			}
		}

		for (AtlasBusinessMetadataDef businessMetadataDef : typesDef.getBusinessMetadataDefs()) {
			if (client.typeWithNameExists(businessMetadataDef.getName())) {
				DorisApp.log(businessMetadataDef.getName() + ": type already exists. Skipping");
			} else {
				typesToCreate.getBusinessMetadataDefs().add(businessMetadataDef);
			}
		}

		return client.createAtlasTypeDefs(typesToCreate);
	}

	private static void deleteTypes(List<String> typeNames, AtlasClientV2 client) throws AtlasServiceException {
		for (String typeName : typeNames) {
			if (!client.typeWithNameExists(typeName)) {
				DorisApp.log(typeName + ": type no exists. Skipping delete");
			}else{
				client.deleteTypeByName(typeName);
			}
		}

	}
}
