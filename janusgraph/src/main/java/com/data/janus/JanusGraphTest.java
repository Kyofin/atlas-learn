package com.data.janus;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardTransactionBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @program: atlas-learn
 * @author: huzekang
 * @create: 2021-11-15 11:57
 **/
public class JanusGraphTest {
	public static final String SEARCH = "search";

	public static void main(String[] args) {
		Map<String, Object> map = new HashMap();
		map.put("storage.backend", "berkeleyje");
		map.put("storage.directory", "/Users/huzekang/study/atlas-learn/db");
		map.put("index." + SEARCH + ".backend", "elastic" + SEARCH);
		map.put("index." + SEARCH + ".hostname", "127.0.0.1");
		final MapConfiguration config = new MapConfiguration(map);
		final StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open(config);


		JanusGraphManagement management = graph.openManagement();

		System.out.println(management.getVertexLabel("person"));
		System.out.println(management.getPropertyKey("name"));

		// 创建属性
		final PropertyKey name = management.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SET).make();
		// 建立唯一索引(只支持等值)
		management.buildIndex("nameUnique", Vertex.class).addKey(name).unique().buildCompositeIndex();
		final PropertyKey age = management.makePropertyKey("age").dataType(Integer.class).make();
		// 建立混合索引(支持非等值，需要索引后端;应用于数值范围查询、全文检索、geo地理位置查询)
		management.buildIndex("AgeIndex", Vertex.class).addKey(age).buildMixedIndex(SEARCH);
		management.buildIndex("NameAndAgeIndex", Vertex.class).addKey(name).addKey(age).buildMixedIndex(SEARCH);
		final PropertyKey reason = management.makePropertyKey("reason").dataType(String.class).make();
		management.buildIndex("ReasonIndex", Edge.class).addKey(reason).buildMixedIndex(SEARCH);


		// 创建顶点标签
		management.makeVertexLabel("person").make();
		management.makeVertexLabel("country").make();
		management.makeVertexLabel("weapon").make();


		// 创建边标签
		management.makeEdgeLabel("brother").make();
		management.makeEdgeLabel("battled").make();
		management.makeEdgeLabel("belongs").signature(reason).make();
		management.makeEdgeLabel("use").make();

		management.commit();

		JanusGraphTransaction tx = graph.newTransaction();
		// 顶点

		Vertex liubei = tx.addVertex(T.label, "person", "name", "刘备", "age", 45);
		Vertex guangyu  = tx.addVertex(T.label, "person", "name", "关羽", "age", 42);
		Vertex zhangfei = tx.addVertex(T.label, "person", "name", "张飞", "age", 40);
		Vertex lvbu = tx.addVertex(T.label, "person", "name", "吕布", "age", 38);
		Vertex shuguo = tx.addVertex(T.label, "country", "name", "蜀国");
		Vertex zbsm = tx.addVertex(T.label, "weapon", "name", "丈八蛇矛");
		Vertex sgj = tx.addVertex(T.label, "weapon", "name", "双股剑");
		Vertex qlyyd = tx.addVertex(T.label, "weapon", "name", "青龙偃月刀");
		Vertex fthj = tx.addVertex(T.label, "weapon", "name", "方天画戟");

		// 边
		liubei.addEdge("brother", guangyu);
		liubei.addEdge("brother", zhangfei);

		liubei.addEdge("belongs", shuguo,"reason","出生在蜀国");
		zhangfei.addEdge("belongs", shuguo,"reason","喜欢蜀国");
		guangyu.addEdge("belongs", shuguo,"reason","在蜀国打工");

		liubei.addEdge("use", sgj);
		zhangfei.addEdge("use", zbsm);
		guangyu.addEdge("use", qlyyd);
		tx.commit();



		final GraphTraversalSource g = graph.traversal();
		System.out.println(g.V().count().next());
		System.out.println( g.V().has("name","刘备").next());
		final Iterator<Vertex> liubeiBrothers = g.V().has("name", "刘备").next().vertices(Direction.OUT, "brother");
		while (liubeiBrothers.hasNext()) {
			final GraphTraversal<Vertex, Vertex> v = g.V(liubeiBrothers.next());
			System.out.println(v.value());
		}

		graph.close();
	}
}
