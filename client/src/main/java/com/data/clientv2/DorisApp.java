package com.data.clientv2;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations;
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

import static org.apache.atlas.type.AtlasTypeUtil.toAtlasRelatedObjectId;
import static org.apache.atlas.type.AtlasTypeUtil.toAtlasRelatedObjectIds;


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
	public static final String METADATA_NAMESPACE_SUFFIX = "@proCluster";


	private static final String ATTR_QUALIFIED_NAME          = "qualifiedName";
	public static final String ATTR_NAME = "name";
	public static final String ATTR_DESCRIPTION = "description";
	public static final String DUPLICATE_TABLE_TYPE = "Duplicate";
	public static final String UNIQUE_TABLE_TYPE = "Unique";
	public static final String AGGREGATE_TABLE_TYPE = "Aggregate";
	public static final String ATTR_SIZE = "size";
	public static final String ATTR_REPLICA_COUNT = "replicaCount";
	public static final String ATTR_CREATE_TIME = "create_time";
	public static final String ATTR_MODEL_TYPE = "modelType";
	public static final String ATTR_DUPLICATE_KEY = "DUPLICATE_KEY";
	public static final String ATTR_AGGREGATE_KEY = "AGGREGATE_KEY";
	public static final String ATTR_UNIQUE_KEY = "UNIQUE_KEY";
	public static final String ATTR_ENGINE = "Engine";
	public static final String ATTR_COMMENT = "comment";
	public static final String ATTR_DB = "db";
	public static final String ATTR_TABLES = "tables";
	public static final String ATTR_COLUMNS = "columns";
	public static final String ATTR_TABLE = "table";
	public static final String ATTR_DATA_TYPE = "dataType";
	public static final String ATTR_IS_NULL = "isNull";

	public static void main(String[] args) throws Exception {
		String[] atlasServerUrls = new String[]{"http://localhost:21000/"};
		String[] basicAuthUsernamePassword = new String[]{"admin", "admin"};
		AtlasClientV2 atlasClientV2 = new AtlasClientV2(atlasServerUrls, basicAuthUsernamePassword);
		createTypes(atlasClientV2);
		//deleteTypes(Lists.newArrayList(
		//		RELAT_DEF_DORIS_DB_TABLES,
		//		RELAT_DEF_DORIS_TB_COLUMNS,
		//		ENTITY_DEF_DORIS_DB,
		//		ENTITY_DEF_DORIS_TABLE,
		//		ENTITY_DEF_DORIS_COLUMN,
		//		ENUM_DEF_DORIS_TABLE_TYPE
		//		),atlasClientV2);

		deleteEntities(atlasClientV2);
		//createDorisEntity(atlasClientV2);
	}

	public static void deleteEntities(AtlasClientV2 atlasClientV2) throws Exception {
		//client.deleteEntityByGuid(loadProcess.getGuid());
		//
		//SampleApp.log("Deleted entity: guid=" + loadProcess.getGuid());

		List<String> entityGuids = Arrays.asList("5b7baac5-a67d-4ad6-819c-b475f9804e44","8896008f-5442-4764-a751-0bc90cf659ad");

		atlasClientV2.deleteEntitiesByGuids(entityGuids);

		DorisApp.log("Deleted entities:");
		for (String entityGuid : entityGuids) {
			DorisApp.log("  guid=" + entityGuid);
		}
	}
	private static void createDorisEntity(AtlasClientV2 atlasClientV2) {
		// 建库
		AtlasEntity dbEntity = new AtlasEntity(ENTITY_DEF_DORIS_DB);
		String dbName = "maoming";
		dbEntity.setAttribute(ATTR_NAME, dbName);
		dbEntity.setAttribute(ATTR_DESCRIPTION, "茂名疫苗数据库");
		dbEntity.setAttribute(ATTR_QUALIFIED_NAME, dbName + METADATA_NAMESPACE_SUFFIX);
		dbEntity.setAttribute("create_time", "1637069312603");
		final AtlasEntityHeader dbEntityResp = createEntity(atlasClientV2, new AtlasEntity.AtlasEntityWithExtInfo(dbEntity));
		// 补充建完db后的guid
		dbEntity.setGuid(dbEntityResp.getGuid());
		DorisApp.log("Created entity: typeName=" + dbEntityResp.getTypeName() + ", qualifiedName=" + dbEntityResp.getAttribute(ATTR_QUALIFIED_NAME) + ", guid=" + dbEntityResp.getGuid());

		// 建表
		AtlasEntity table1Entity = new AtlasEntity(ENTITY_DEF_DORIS_TABLE);
		final String table1Name = "maoming_info";
		table1Entity.setAttribute(ATTR_NAME, table1Name);
		table1Entity.setAttribute(ATTR_DESCRIPTION, "疫苗基础表");
		table1Entity.setAttribute(ATTR_QUALIFIED_NAME, dbEntity.getAttribute(ATTR_NAME) + "." + table1Name + METADATA_NAMESPACE_SUFFIX);
		table1Entity.setAttribute(ATTR_MODEL_TYPE, DUPLICATE_TABLE_TYPE);
		table1Entity.setAttribute(ATTR_SIZE, "11152956325");
		table1Entity.setAttribute(ATTR_REPLICA_COUNT, "288");
		table1Entity.setAttribute(ATTR_CREATE_TIME, "1637069312603");
		table1Entity.setAttribute(ATTR_MODEL_TYPE, DUPLICATE_TABLE_TYPE);
		table1Entity.setAttribute(ATTR_DUPLICATE_KEY, "person_name");
		table1Entity.setAttribute(ATTR_ENGINE, "OLAP");
		table1Entity.setAttribute(ATTR_COMMENT, "疫苗基础表");
		// 关联库
		final AtlasRelatedObjectId dbAtlasRelatedObjectId = AtlasTypeUtil.getAtlasRelatedObjectId(AtlasTypeUtil.getObjectId(dbEntity), RELAT_DEF_DORIS_DB_TABLES);
		table1Entity.setRelationshipAttribute(ATTR_DB, dbAtlasRelatedObjectId);

		// 建字段
		final List<AtlasEntity> columnEntities = Arrays.asList(
				createColumn("person_name","varchar(254)","",true),
				createColumn("gender_code","varchar(254)","",true),
				createColumn("birthdate","date","",true),
				createColumn("admin_org","varchar(254)","",true),
				createColumn("work_unit","varchar(254)","",true),
				createColumn("live_addres","varchar(254)","",true),
				createColumn("person_type","varchar(254)","",true),
				createColumn("inject_date","date","",true),
				createColumn("inject_time","varchar(254)","",true),
				createColumn("com_short_n","varchar(254)","",true),
				createColumn("vaccine_typ","varchar(254)","",true),
				createColumn("organ_name","varchar(254)","",true),
				createColumn("input_area_","varchar(254)","",true),
				createColumn("record_area","varchar(254)","",true),
				createColumn("id","varchar(254)","",true)
		);
		// 关联字段
		table1Entity.setRelationshipAttribute(ATTR_COLUMNS, toAtlasRelatedObjectIds(columnEntities));

		AtlasEntity.AtlasEntityWithExtInfo table1EntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
		table1EntityWithExtInfo.setEntity(table1Entity);
		for (AtlasEntity column : columnEntities) {
			column.setRelationshipAttribute(ATTR_TABLE, toAtlasRelatedObjectId(table1Entity));
			table1EntityWithExtInfo.addReferredEntity(column);
		}
		AtlasEntityHeader createdHeader = createEntity(atlasClientV2,table1EntityWithExtInfo);
		DorisApp.log("Created entity: typeName=" + createdHeader.getTypeName() + ", qualifiedName=" + createdHeader.getAttribute(ATTR_QUALIFIED_NAME) + ", guid=" + createdHeader.getGuid());




	}

	private static AtlasEntity createColumn(String name, String dataType, String comment,boolean isNull ) {
		AtlasEntity ret = new AtlasEntity(ENTITY_DEF_DORIS_COLUMN);

		ret.setAttribute(ATTR_NAME, name);
		ret.setAttribute(ATTR_QUALIFIED_NAME, name + METADATA_NAMESPACE_SUFFIX);
		ret.setAttribute(ATTR_DATA_TYPE, dataType);
		ret.setAttribute(ATTR_IS_NULL, isNull);
		ret.setAttribute(ATTR_COMMENT, comment);

		return ret;
	}

	private static AtlasEntityHeader createEntity(AtlasClientV2 atlasClientV2,AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo) {
		EntityMutationResponse entity;

		try {
			entity = atlasClientV2.createEntity(atlasEntityWithExtInfo);

			if (entity != null && entity.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE) != null) {
				List<AtlasEntityHeader> list = entity.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE);

				if (list.size() > 0) {
					return entity.getEntitiesByOperation(EntityMutations.EntityOperation.CREATE).get(0);
				}
			}
		} catch (AtlasServiceException e) {
			DorisApp.log("failed in create entity");
			e.printStackTrace();
		}

		return null;
	}

	private static void createTypes(AtlasClientV2 atlasClientV2) throws AtlasServiceException {
		// 定义doris 库entity的属性
		final List<AtlasStructDef.AtlasAttributeDef> dbAtlasAttributeDefs = Arrays.asList(
				AtlasTypeUtil.createOptionalAttrDef("create_time", "date")
		);
		// 定义doris 表entity的属性
		final List<AtlasStructDef.AtlasAttributeDef> tbAtlasAttributeDefs = Arrays.asList(
				AtlasTypeUtil.createOptionalAttrDef(ATTR_SIZE, "long"),
				AtlasTypeUtil.createOptionalAttrDef(ATTR_REPLICA_COUNT, "int"),
				AtlasTypeUtil.createOptionalAttrDef(ATTR_CREATE_TIME, "date"),
				AtlasTypeUtil.createOptionalAttrDef(ATTR_MODEL_TYPE, ENUM_DEF_DORIS_TABLE_TYPE),
				AtlasTypeUtil.createOptionalAttrDef(ATTR_DUPLICATE_KEY, "string"),
				AtlasTypeUtil.createOptionalAttrDef(ATTR_AGGREGATE_KEY, "string"),
				AtlasTypeUtil.createOptionalAttrDef(ATTR_UNIQUE_KEY, "string"),
				AtlasTypeUtil.createOptionalAttrDef(ATTR_ENGINE, "string"),
				AtlasTypeUtil.createOptionalAttrDef(ATTR_COMMENT, "string")
		);
		// 定义doris 库entity模板
		final AtlasEntityDef dorisDbEntityDef = new AtlasEntityDef(ENTITY_DEF_DORIS_DB, null, "1.0", dbAtlasAttributeDefs, Sets.newHashSet(ENTITY_TYPE_DATASET));
		// 定义doris 表entity模板
		final AtlasEntityDef dorisTableEntityDef = new AtlasEntityDef(ENTITY_DEF_DORIS_TABLE, null, "1.0", tbAtlasAttributeDefs, Sets.newHashSet(ENTITY_TYPE_DATASET));
		// 定义doris 字段entity模板
		final AtlasEntityDef dorisColumnEntityDef = AtlasTypeUtil.createClassTypeDef(ENTITY_DEF_DORIS_COLUMN,
				Collections.singleton(ENTITY_TYPE_DATASET),
				AtlasTypeUtil.createOptionalAttrDef(ATTR_IS_NULL, "boolean"),
				AtlasTypeUtil.createOptionalAttrDef(ATTR_DATA_TYPE, "string"),
				AtlasTypeUtil.createOptionalAttrDef(ATTR_COMMENT, "string")
		);
		// 定义doris 表模型的枚举值
		final AtlasEnumDef dorisTableTypeEnumDef = new AtlasEnumDef(ENUM_DEF_DORIS_TABLE_TYPE, null,
				Arrays.asList(
						new AtlasEnumDef.AtlasEnumElementDef(DUPLICATE_TABLE_TYPE, null, 1),
						new AtlasEnumDef.AtlasEnumElementDef(UNIQUE_TABLE_TYPE, null, 2),
						new AtlasEnumDef.AtlasEnumElementDef(AGGREGATE_TABLE_TYPE, null, 3)
				)
		);
		// 定义doris 表和库的关系
		final AtlasRelationshipDef dbTablesDef = new AtlasRelationshipDef(RELAT_DEF_DORIS_DB_TABLES, null, "1.0",
				AtlasRelationshipDef.RelationshipCategory.AGGREGATION, AtlasRelationshipDef.PropagateTags.NONE,
				new AtlasRelationshipEndDef(ENTITY_DEF_DORIS_TABLE, ATTR_DB, AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, false),
				AtlasTypeUtil.createRelationshipEndDef(ENTITY_DEF_DORIS_DB, ATTR_TABLES, AtlasStructDef.AtlasAttributeDef.Cardinality.SET, true));
		// 定义doris 表和字段的关系
		final AtlasRelationshipDef tableColumnsDef = new AtlasRelationshipDef(RELAT_DEF_DORIS_TB_COLUMNS, null, "1.0",
				AtlasRelationshipDef.RelationshipCategory.COMPOSITION, AtlasRelationshipDef.PropagateTags.NONE,
				new AtlasRelationshipEndDef(ENTITY_DEF_DORIS_TABLE, ATTR_COLUMNS, AtlasStructDef.AtlasAttributeDef.Cardinality.SET, true),
				AtlasTypeUtil.createRelationshipEndDef(ENTITY_DEF_DORIS_COLUMN, ATTR_TABLE, AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE, false));
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
