package com.data.clientv2;

/**
 * @program: atlas-learn
 * @author: huzekang
 * @create: 2021-11-18 18:18
 **/
public class QueryPlan {
	public static String content = "PLAN FRAGMENT 0\n" +
			" OUTPUT EXPRS:<slot 11> `admin_org` | <slot 12> `person_type` | <slot 13> `com_short_n` | <slot 14> `gender_code` | <slot 15> sum(`default_cluster:maoming`.`maoming_info`.`mv_count_person_name`)\n" +
			"  PARTITION: HASH_PARTITIONED: <slot 11> `admin_org`, <slot 12> `person_type`, <slot 13> `com_short_n`, <slot 14> `gender_code`\n" +
			"\n" +
			"  OLAP TABLE SINK\n" +
			"    TUPLE ID: 0\n" +
			"    RANDOM\n" +
			"\n" +
			"  3:AGGREGATE (merge finalize)\n" +
			"  |  output: sum(<slot 15> sum(`default_cluster:maoming`.`maoming_info`.`mv_count_person_name`))\n" +
			"  |  group by: <slot 11> `admin_org`, <slot 12> `person_type`, <slot 13> `com_short_n`, <slot 14> `gender_code`\n" +
			"  |  cardinality=-1\n" +
			"  |  tuple ids: 2 \n" +
			"  |  \n" +
			"  2:EXCHANGE\n" +
			"     tuple ids: 2 \n" +
			"\n" +
			"PLAN FRAGMENT 1\n" +
			" OUTPUT EXPRS:\n" +
			"  PARTITION: HASH_PARTITIONED: `default_cluster:maoming`.`maoming_info`.`id`\n" +
			"\n" +
			"  STREAM DATA SINK\n" +
			"    EXCHANGE ID: 02\n" +
			"    HASH_PARTITIONED: <slot 11> `admin_org`, <slot 12> `person_type`, <slot 13> `com_short_n`, <slot 14> `gender_code`\n" +
			"\n" +
			"  1:AGGREGATE (update serialize)\n" +
			"  |  STREAMING\n" +
			"  |  output: sum(`default_cluster:maoming`.`maoming_info`.`mv_count_person_name`)\n" +
			"  |  group by: `admin_org`, `person_type`, `com_short_n`, `gender_code`\n" +
			"  |  cardinality=-1\n" +
			"  |  tuple ids: 2 \n" +
			"  |  \n" +
			"  0:OlapScanNode\n" +
			"     TABLE: maoming_info\n" +
			"     PREAGGREGATION: ON\n" +
			"     partitions=1/1\n" +
			"     rollup: maoming_person_uv\n" +
			"     tabletRatio=32/32\n" +
			"     tabletList=58978,58982,58986,58990,58994,58998,59002,59006,59010,59014 ...\n" +
			"     cardinality=390122\n" +
			"     avgRowSize=72.0\n" +
			"     numNodes=3\n" +
			"     tuple ids: 1 \n" +
			"\n" +
			"Tuples:\n" +
			"TupleDescriptor{id=0, tbl=null, byteSize=72, materialized=true}\n" +
			"  SlotDescriptor{id=0, col=admin_org, type=VARCHAR(*)}\n" +
			"    parent=0\n" +
			"    materialized=true\n" +
			"    byteSize=16\n" +
			"    byteOffset=8\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=1\n" +
			"    slotIdx=1\n" +
			"\n" +
			"  SlotDescriptor{id=1, col=person_type, type=VARCHAR(*)}\n" +
			"    parent=0\n" +
			"    materialized=true\n" +
			"    byteSize=16\n" +
			"    byteOffset=24\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=2\n" +
			"    slotIdx=2\n" +
			"\n" +
			"  SlotDescriptor{id=2, col=com_short_n, type=VARCHAR(*)}\n" +
			"    parent=0\n" +
			"    materialized=true\n" +
			"    byteSize=16\n" +
			"    byteOffset=40\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=3\n" +
			"    slotIdx=3\n" +
			"\n" +
			"  SlotDescriptor{id=3, col=gender_code, type=VARCHAR(*)}\n" +
			"    parent=0\n" +
			"    materialized=true\n" +
			"    byteSize=16\n" +
			"    byteOffset=56\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=4\n" +
			"    slotIdx=4\n" +
			"\n" +
			"  SlotDescriptor{id=4, col=person_count, type=INT}\n" +
			"    parent=0\n" +
			"    materialized=true\n" +
			"    byteSize=4\n" +
			"    byteOffset=4\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=0\n" +
			"    slotIdx=0\n" +
			"\n" +
			"\n" +
			"TupleDescriptor{id=1, tbl=maoming_info, byteSize=80, materialized=true}\n" +
			"  SlotDescriptor{id=5, col=admin_org, type=VARCHAR(*)}\n" +
			"    parent=1\n" +
			"    materialized=true\n" +
			"    byteSize=16\n" +
			"    byteOffset=16\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=0\n" +
			"    slotIdx=1\n" +
			"\n" +
			"  SlotDescriptor{id=6, col=person_type, type=VARCHAR(*)}\n" +
			"    parent=1\n" +
			"    materialized=true\n" +
			"    byteSize=16\n" +
			"    byteOffset=32\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=1\n" +
			"    slotIdx=2\n" +
			"\n" +
			"  SlotDescriptor{id=7, col=com_short_n, type=VARCHAR(*)}\n" +
			"    parent=1\n" +
			"    materialized=true\n" +
			"    byteSize=16\n" +
			"    byteOffset=48\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=2\n" +
			"    slotIdx=3\n" +
			"\n" +
			"  SlotDescriptor{id=8, col=gender_code, type=VARCHAR(*)}\n" +
			"    parent=1\n" +
			"    materialized=true\n" +
			"    byteSize=16\n" +
			"    byteOffset=64\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=3\n" +
			"    slotIdx=4\n" +
			"\n" +
			"  SlotDescriptor{id=9, col=person_name, type=VARCHAR(*)}\n" +
			"    parent=1\n" +
			"    materialized=false\n" +
			"    byteSize=0\n" +
			"    byteOffset=-1\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=0\n" +
			"    slotIdx=0\n" +
			"\n" +
			"  SlotDescriptor{id=10, col=mv_count_person_name, type=BIGINT}\n" +
			"    parent=1\n" +
			"    materialized=true\n" +
			"    byteSize=8\n" +
			"    byteOffset=8\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=-1\n" +
			"    slotIdx=0\n" +
			"\n" +
			"\n" +
			"TupleDescriptor{id=2, tbl=null, byteSize=80, materialized=true}\n" +
			"  SlotDescriptor{id=11, col=null, type=VARCHAR(*)}\n" +
			"    parent=2\n" +
			"    materialized=true\n" +
			"    byteSize=16\n" +
			"    byteOffset=16\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=1\n" +
			"    slotIdx=1\n" +
			"\n" +
			"  SlotDescriptor{id=12, col=null, type=VARCHAR(*)}\n" +
			"    parent=2\n" +
			"    materialized=true\n" +
			"    byteSize=16\n" +
			"    byteOffset=32\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=2\n" +
			"    slotIdx=2\n" +
			"\n" +
			"  SlotDescriptor{id=13, col=null, type=VARCHAR(*)}\n" +
			"    parent=2\n" +
			"    materialized=true\n" +
			"    byteSize=16\n" +
			"    byteOffset=48\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=3\n" +
			"    slotIdx=3\n" +
			"\n" +
			"  SlotDescriptor{id=14, col=null, type=VARCHAR(*)}\n" +
			"    parent=2\n" +
			"    materialized=true\n" +
			"    byteSize=16\n" +
			"    byteOffset=64\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=4\n" +
			"    slotIdx=4\n" +
			"\n" +
			"  SlotDescriptor{id=15, col=null, type=BIGINT}\n" +
			"    parent=2\n" +
			"    materialized=true\n" +
			"    byteSize=8\n" +
			"    byteOffset=8\n" +
			"    nullIndicatorByte=0\n" +
			"    nullIndicatorBit=0\n" +
			"    slotIdx=0";
}
