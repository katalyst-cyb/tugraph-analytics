import com.antgroup.geaflow.api.context.RuntimeContext;
import com.antgroup.geaflow.api.function.RichFunction;
import com.antgroup.geaflow.api.function.aggregation.Aggregator;
import com.antgroup.geaflow.api.graph.*;
import com.antgroup.geaflow.api.graph.compute.VertexCentricCompute;
import com.antgroup.geaflow.api.graph.function.vc.VertexCentricCombineFunction;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowStream;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.example.util.ResultValidator;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.antgroup.geaflow.model.graph.edge.impl.ValueEdge;
import com.antgroup.geaflow.model.graph.vertex.Vertex;

import java.util.Collections;
import java.util.Iterator;

public class PersonalizedPageRank {

    // 自定义VertexCentricCompute实现PPR逻辑
    public static class PPRCompute extends RichFunction implements VertexCentricCompute<Long, Double, Double, Double> {

        private final double alpha;
        private final int maxIterations;
        private final long targetId;
        private RuntimeContext runtimeContext;

        public PPRCompute(double alpha, int maxIterations, long targetId) {
            this.alpha = alpha;
            this.maxIterations = maxIterations;
            this.targetId = targetId;
        }

        @Override
        public void open(RuntimeContext runtimeContext, Configuration configuration) {
            this.runtimeContext = runtimeContext;
        }

        @Override
        public void compute(Long vertexId, Iterator<Double> messageIterator) {
            Vertex<Long, Double> vertex = this.context.vertex().get();

            if (this.context.getIterationId() == 1L) {
                // 初始化：目标节点值为1，其他为0
                double initValue = vertex.getId().equals(targetId) ? 1.0 : 0.0;
                vertex.setValue(initValue);
            } else {
                // 聚合消息
                double sum = 0.0;
                while (messageIterator.hasNext()) {
                    sum += messageIterator.next();
                }
                // 计算PPR公式
                double personalization = vertex.getId().equals(targetId) ? 1.0 : 0.0;
                double newRank = alpha * sum + (1 - alpha) * personalization;
                vertex.setValue(newRank);
            }

            // 发送消息到出边邻居
            if (!vertex.getOutEdges().isEmpty()) {
                double outValue = vertex.getValue() / vertex.getOutEdges().size();
                for (Edge<Long, Double> edge : vertex.getOutEdges()) {
                    this.context.sendMessage(edge.getTargetId(), outValue);
                }
            }
        }

        @Override
        public int getMaxIteration() {
            return this.maxIterations;
        }
    }

    // 构建图并执行PPR计算
    public static void execute(GraphViewDesc graphDesc, long targetNodeId) {
        // 参数设置
        double alpha = 0.85;
        int maxIterations = 20;

        // 构建图结构（示例数据）
        PWindowStream<Vertex<Long, Double>> vertices = ...; // 顶点数据源
        PWindowStream<ValueEdge<Long, Double>> edges = ...; // 边数据源

        // 创建图
        Graph<Long, Double, Double> graph = graphBuilder.buildGraph(
                vertices, edges,
                new DefaultGraphLoader<Long, Double, Double>() {
                    @Override
                    public Double loadVertexValue(Long vertexId) {
                        return 0.0; // 初始值会被PPRCompute覆盖
                    }

                    @Override
                    public Double loadEdgeValue(Long srcId, Long targetId) {
                        return 1.0; // 边权重默认为1
                    }
                });

        // 执行PPR计算
        VertexCentricTraversal<Long, Double, Double, Double, Double> traversal = graph
                .traversal(new PPRCompute(alpha, maxIterations, targetNodeId))
                .withAggregator(new Aggregator<Double>() {
                    // 可选：定义聚合器监控收敛情况
                });

        // 触发执行并获取结果
        PWindowStream<Vertex<Long, Double>> result = traversal.vertex().getVertices();
        result.print(); // 输出结果
    }

    public static void main(String[] args) {
        GraphViewDesc graphDesc = GraphViewBuilder.createGraphView("ppr_graph");
        execute(graphDesc, 1L); // 假设目标节点ID为1
    }
}
