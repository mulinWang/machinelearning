package com.longyuan.machinelearning.test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.*;

/**
 * Created by mulin on 2017/11/8.
 * spark基础用例，spark入门
 */
public class JavaSparkBasicExample {

    private static JavaSparkContext JSC = null;

    @BeforeClass
    public static void setUp() {
        SparkConf conf = new SparkConf().setAppName("UsageMain").setMaster("local[*]");
        JSC = new JavaSparkContext(conf);
    }

    @AfterClass
    public static void tearDown() {
        JSC.close();
    }

    /**
     * 测试 map函数
     * map是对RDD中的每个元素都执行一个指定的函数来产生一个新的RDD。 任何原RDD中的元素在新RDD中都有且只有一个元素与之对应。
     */
    @Test
    public void mapTest() {
        JavaRDD<Integer> rdd = JSC.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);

        JavaRDD<Integer> result = rdd.map(f -> f * 2);
        System.out.println("[测试 map函数]MAP返回结果:" + result.count());
        result.foreach(id -> System.out.println(id + ";"));
        System.out.println("===========================================================");
    }

    /**
     * 测试mapPartition函数
     * mapPartitions是map的一个变种。map的输入函数是应用于RDD中每个元素，
     * 而mapPartitions的输入函数是应用于每个分区，也就是把每个分区中的内容作为整体来处理的。
     */
    @Test
    public void mapPartitionTest() {
        JavaRDD<Integer> rdd = JSC.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), 3);
        System.out.println("rdd2分区大小:" + rdd.partitions().size());
        JavaRDD<Integer> result = rdd.mapPartitions((FlatMapFunction<Iterator<Integer>, Integer>) t -> {
            List<Integer> list = new ArrayList<Integer>();
            int a = 0;
            while (t.hasNext()) {
                a += t.next();
            }
            list.add(a);
            return list.iterator();
        }, true);

        System.out.println("result1分区大小:" + result.partitions());
        System.out.println("[测试mapPartition函数]mapPartition返回结果:" + result.count());
        result.foreach(id -> System.out.println(id));
        System.out.println("===========================================================");

        JavaRDD<Tuple2<Integer, Integer>> result3 = rdd.mapPartitions(t -> {
            List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
            int key = t.next();
            while (t.hasNext()) {
                int value = t.next();
                list.add(new Tuple2<Integer, Integer>(key, value));
                key = value;
            }
            return list.iterator();
        });
        System.out.println("[测试mapPartition函数, 另一种写法]返回结果:" + result3.count());
        result3.foreach(id -> System.out.println(id));
        System.out.println("===========================================================");
    }

    /**
     * 测试mapValues函数
     * mapValues顾名思义就是输入函数应用于RDD中Kev-Value的Value，原RDD中的Key保持不变，与新的Value一起组成新的RDD中的元素。
     * 因此，该函数只适用于元素为KV对的RDD。
     */
    @Test
    public void mapValuesTest() {
        Tuple2<Integer, Integer> t1 = new Tuple2<Integer, Integer>(1, 3);
        Tuple2<Integer, Integer> t2 = new Tuple2<Integer, Integer>(1, 2);
        Tuple2<Integer, Integer> t3 = new Tuple2<Integer, Integer>(1, 4);
        Tuple2<Integer, Integer> t4 = new Tuple2<Integer, Integer>(2, 3);
        List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
        list.add(t1);
        list.add(t2);
        list.add(t3);
        list.add(t4);

        JavaPairRDD<Integer, Integer> rddValue = JSC.parallelizePairs(list);
        System.out.println("分片数:"+rddValue.partitions().size());
        JavaPairRDD<Integer, Integer> resultValue = rddValue.mapValues(v -> v * 2);
        System.out.println("[测试mapValues函数]返回结果:");
        resultValue.foreach(t -> {
            System.out.println(t._1 + "=" + t._2);
        });
        System.out.println("===========================================================");
    }

    /***
     * 测试mapPartitionsWithIndex函数
     * mapPartitionsWithIndex是函数作用同mapPartitions，不过提供了两个参数，第一个参数为分区的索引。
     */
    @Test
    public void mapPartitionsWithIndexTest() {
        JavaRDD<Integer> rddWith = JSC.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);
        JavaRDD<String> rddResult = rddWith.mapPartitionsWithIndex((x, it) -> {
            List<String> midList = new ArrayList<String>();
            int a = 0;
            while (it.hasNext()) {
                a += it.next();
            }
            midList.add(x + "|" + a);
            return midList.iterator();
        }, false);
        System.out.println("[测试mapPartitionsWithIndex函数]返回结果:" + rddResult.count());
        System.out.println(rddResult.collect());
    }

    /**
     * 测试 flatMap函数
     * 与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，而原RDD中的元素经flatmap处理后可生成多个元素来构建新RDD。
     * 举例：对原RDD中的每个元素x产生y个元素（从1到y，y为元素x的值）
     */
    @Test
    public void flatMapTest() {
        JavaRDD<Integer> rddFlatMap = JSC.parallelize(Arrays.asList(1, 2, 3, 4), 2);
        JavaRDD<Integer> rddFlatMapResult = rddFlatMap.flatMap(t -> {
            List<Integer> listFlat = new ArrayList<Integer>();
            for (int i = 1; i <= t; i++) {
                listFlat.add(i);
            }
            return listFlat.iterator();
        });
        System.out.println("[测试flatMap函数]返回结果:" + rddFlatMapResult.count());
        System.out.println(rddFlatMapResult.collect());
    }

    /**
     * aggregate函数将每个分区里面的元素进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
     * 这个函数最终返回的类型不需要和RDD中元素类型一致。
     * 执行步骤：
     * 1: 零值11参与 seqOp 方法的执行，
     * 2: seqOp 方法执行完成之后，零值11继续参与 combOp方法的执行
     * 3: 具体执行步骤 如下:
     * seqOp:11,1
     * seqOp:1,2
     * seqOp:1,3
     * combOp:11,1
     * seqOp:11,4
     * seqOp:4,5
     * seqOp:4,6
     * combOp:12,4
     * 聚合返回结果:16
     */
    @Test
    public void aggregateTest() {
        JavaRDD<Integer> rdd1 = JSC.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);
        int result = rdd1.aggregate(11, (a, b) -> {
            System.out.println("seqOp:" + a + "," + b);
            return Math.min(a, b);
        }, (a, b) -> {
            System.out.println("combOp:" + a + "," + b);
            return a + b;
        });
        rdd1.foreach(t -> System.out.println(t));
        System.out.println("聚合返回结果:" + result);
    }

    /**
     * aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。
     * 和aggregate函数类似，aggregateByKey返回值的类型不需要和RDD中value的类型一致。
     * 因为aggregateByKey是对相同Key中的值进行聚合操作，所以aggregateByKey函数最终返回的类型还是Pair RDD，
     * 对应的结果是Key和聚合好的值；而aggregate函数直接是返回非RDD的结果，这点需要注意。
     * 在实现过程中，定义了三个aggregateByKey函数原型，但最终调用的aggregateByKey函数都一致。
     * <p>
     * aggregateByKey和aggregate结果有点不一样。
     * 如果用aggregate函数对含有3、2、4三个元素的RDD进行计算，初始值为1的时候，计算的结果应该是10，而这里是9，
     * 这是因为aggregate函数中的初始值需要和reduce函数以及combine函数结合计算，而aggregateByKey中的初始值只需要和reduce函数计算，
     * 不需要和combine函数结合计算，所以导致结果有点不一样。
     */
    @Test
    public void aggregateByKeyTest() {
        Tuple2<Integer, Integer> t1 = new Tuple2<>(1, 3);
        Tuple2<Integer, Integer> t2 = new Tuple2<>(1, 2);
        Tuple2<Integer, Integer> t3 = new Tuple2<>(1, 4);
        Tuple2<Integer, Integer> t4 = new Tuple2<>(2, 3);
        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
        System.out.println("原始数据如下:");
        list.add(t1);
        list.add(t2);
        list.add(t3);
        list.add(t4);

        list.forEach(t -> System.out.println(t._1 + "," + t._2));
        System.out.println("====================================");

        JavaPairRDD<Integer, Integer> rdd1 = JSC.parallelizePairs(list, 4);
        JavaPairRDD<Integer, Integer> result = rdd1.aggregateByKey(11, (a, b) -> {
            System.out.println("seqOp:" + a + "," + b);
            return Math.max(a, b);
        }, (a, b) -> {
            System.out.println("combOp:" + a + "," + b);
            return a + b;
        });
        result.foreach(t -> System.out.println(t));
        System.out.println("聚合返回结果:" + result.collectAsMap());
    }

    /**
     * reduce将RDD中元素两两传递给输入函数，同时产生一个新的值，新产生的值与RDD中下一个元素再被传递给输入函数直到最后只有一个值为止。
     */
    @Test
    public void reduceTest() {
        JavaRDD<Integer> rdd1 = JSC.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);
        // 测试reduce函数
        Integer result = rdd1.reduce((m, n) -> m + n);
        System.out.println("[测试 reduce函数]返回结果:" + result);

    }

    /**
     * fold和reduce的原理相同，但是与reduce不同，相当于每个reduce时，迭代器取的第一个元素是zeroValue。
     */
    @Test
    public void foldTest() {
        JavaRDD<Integer> rddFold = JSC.parallelize(Arrays.asList(1, 2, 3, 4));
        int resultFold = rddFold.fold(3, (a, b) -> {
            System.out.println(a + "+" + b + "=" + (a + b));
            return a + b;
        });
        System.out.println("[测试 fold 函数]返回结果:" + resultFold);
    }

    /**
     * 顾名思义，reduceByKey就是对元素为KV对的RDD中Key相同的元素的Value进行reduce，因此，Key相同的多个元素的值被reduce为一个值，然后与原RDD中的Key组成一个新的KV对。
     */
    @Test
    public void reduceByKeyTest() {
        Tuple2<Integer, Integer> t1 = new Tuple2<>(1, 3);
        Tuple2<Integer, Integer> t2 = new Tuple2<>(1, 2);
        Tuple2<Integer, Integer> t3 = new Tuple2<>(2, 4);
        Tuple2<Integer, Integer> t4 = new Tuple2<>(2, 3);
        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
        System.out.println("[测试 reduceByKey函数]原始数据如下<1>:");
        list.add(t1);
        list.add(t2);
        list.add(t3);
        list.add(t4);

        list.forEach(t -> System.out.println(t._1 + "," + t._2()));

        Tuple2<Integer, Integer> p1 = new Tuple2<>(3, 3);
        Tuple2<Integer, Integer> p2 = new Tuple2<>(4, 2);
        Tuple2<Integer, Integer> p3 = new Tuple2<>(5, 4);
        Tuple2<Integer, Integer> p4 = new Tuple2<>(5, 3);
        List<Tuple2<Integer, Integer>> listP = new ArrayList<>();
        System.out.println("[测试 reduceByKey函数]原始数据如下<2>:");
        listP.add(p1);
        listP.add(p2);
        listP.add(p3);
        listP.add(p4);

        listP.forEach(t -> System.out.println(t._1 + "," + t._2()));
        System.out.println("====================================");
        // 测试reduceByKey函数
        JavaPairRDD<Integer, Integer> rdd = JSC.parallelizePairs(list);
        JavaPairRDD<Integer, Integer> rddp = JSC.parallelizePairs(listP);

        JavaPairRDD<Integer, Integer> result = rdd.union(rddp).reduceByKey((m, n) -> m + n);
        System.out.println("[测试 reduceByKey函数]返回结果:" + result.collectAsMap());
    }

    /**
     * cartesian就是笛卡尔积运算
     * [测试 cartesian函数]返回结果:[(1,4), (1,5), (1,6), (2,4), (2,5), (2,6), (3,4), (3,5), (3,6)]
     */
    @Test
    public void cartesianTest() {
        // 测试cartesian函数
        JavaRDD<Integer> rdd1 = JSC.parallelize(Arrays.asList(1, 2, 3));
        JavaRDD<Integer> rdd2 = JSC.parallelize(Arrays.asList(4, 5, 6));

        JavaPairRDD<Integer, Integer> result = rdd1.cartesian(rdd2);
        System.out.println("[测试 cartesian函数]返回结果:" + result.collect());
    }

    /**
     * glom 函数将每一个分区设置成一个数组
     * [测试 glom函数]返回结果:[[1, 2, 3], [4, 5, 6]]
     */
    @Test
    public void glomTest() {
        JavaRDD<Integer> rdd1 = JSC.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);
        JavaRDD<List<Integer>> result = rdd1.glom();

        System.out.println("[测试 glom函数]返回结果:" + result.collect());
    }

      /**
     * subtract 函数  对两个RDD中的集合进行差运算
     * [测试 subtract 函数]返回结果:[2, 4, 1, 3]
     */
    @Test
    public void subtractTest() {
        JavaRDD<Integer> rdd1 = JSC.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);
        JavaRDD<Integer> rdd2 = JSC.parallelize(Arrays.asList(5, 6), 2);
        JavaRDD<Integer> result = rdd1.subtract(rdd2, 4);// 第二个参数表示结果RDD的分区数

        System.out.println("[测试 subtract 函数]返回结果:" + result.collect());
        System.out.println("[测试 subtract 函数]返回结果:" + result.partitions().size());
    }

    /**
     * sample 采样函数
     * [测试 sample 函数]返回结果:[2, 4, 7, 9]
     * [测试 takeSample 函数]返回结果: 4 1 2
     */
    @Test
    public void sampleTest() {
        JavaRDD<Integer> rdd1 = JSC.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        // true 表示有放回去的抽样
        // false 表示没有放回去的抽样
        // 第二个参数为采样率 在 0->1 之间
        JavaRDD<Integer> result = rdd1.sample(false, 0.4);
        System.out.println("[测试 sample 函数]返回结果:" + result.collect());

        // 第一个参数和sample函数是相同的，第二个参数表示采样的个数
        List<Integer> result1 = rdd1.takeSample(false, 3);
        System.out.println("[测试 takeSample 函数]返回结果:");
        result1.forEach(System.out::print);
    }

    /**
     * combineByKey 函数测试
     * [combineByKey测试]返回结果:{2=[5, 3], 1=[4, 2, 3]}
     */
    @Test
    public void combineByKeyTest() {
        Tuple2<Integer, Integer> t1 = new Tuple2<Integer, Integer>(1, 3);
        Tuple2<Integer, Integer> t2 = new Tuple2<Integer, Integer>(1, 2);
        Tuple2<Integer, Integer> t3 = new Tuple2<Integer, Integer>(1, 4);
        Tuple2<Integer, Integer> t4 = new Tuple2<Integer, Integer>(2, 3);
        Tuple2<Integer, Integer> t5 = new Tuple2<Integer, Integer>(2, 3);
        Tuple2<Integer, Integer> t6 = new Tuple2<Integer, Integer>(2, 5);
        List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
        System.out.println("[combineByKey测试]原始数据如下:");
        list.add(t1);
        list.add(t2);
        list.add(t3);
        list.add(t4);
        list.add(t5);
        list.add(t6);

        list.forEach(t -> {
            System.out.println(t._1 + "," + t._2());
        });
        System.out.println("====================================");

        JavaPairRDD<Integer, Integer> rdd1 = JSC.parallelizePairs(list);
        JavaPairRDD<Integer, Set<Integer>> result = rdd1.combineByKey(v -> {
            // 如果集合不存在则创建集合
            Set<Integer> h = new HashSet<Integer>();
            h.add(v);
            return h;
        }, (h, v) -> {
            // 如果集合存在则添加数据
            h.add(v);
            return h;
        }, (vl, vr) -> {
            // 合并两个集合
            vl.addAll(vr);
            return vl;
        });

        rdd1.foreach(System.out::println);
        System.out.println("[combineByKey测试]返回结果:" + result.collectAsMap());
    }

    /**
     * cogroup函数原型一共有九个,  最多可以组合四个RDD。
     * <p>
     * [测试 cogroup函数]原始数据如下<1>:
     * 1,3 | 1,2 | 2,4 | 2,3 | 2,3 |
     * [测试 cogroup函数]原始数据如下<2>:
     * 3,3 | 4,2 | 5,4 | 5,3 | ====================================
     * [测试 cogroup函数]返回结果:[(4,([],[2])), (1,([3, 2],[])), (5,([],[4, 3])), (2,([4, 3, 3],[])), (3,([],[3]))]
     */
    @Test
    public void cogroupTest() {
        Tuple2<Integer, Integer> t1 = new Tuple2<Integer, Integer>(1, 3);
        Tuple2<Integer, Integer> t2 = new Tuple2<Integer, Integer>(1, 2);
        Tuple2<Integer, Integer> t3 = new Tuple2<Integer, Integer>(2, 4);
        Tuple2<Integer, Integer> t4 = new Tuple2<Integer, Integer>(2, 3);
        Tuple2<Integer, Integer> t5 = new Tuple2<Integer, Integer>(2, 3);
        List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
        System.out.println("[测试 cogroup函数]原始数据如下<1>:");
        list.add(t1);
        list.add(t2);
        list.add(t3);
        list.add(t4);
        list.add(t5);

        list.forEach(t -> {
            System.out.print(t._1 + "," + t._2() + " | ");
        });

        Tuple2<Integer, Integer> p1 = new Tuple2<Integer, Integer>(3, 3);
        Tuple2<Integer, Integer> p2 = new Tuple2<Integer, Integer>(4, 2);
        Tuple2<Integer, Integer> p3 = new Tuple2<Integer, Integer>(5, 4);
        Tuple2<Integer, Integer> p4 = new Tuple2<Integer, Integer>(5, 3);
        List<Tuple2<Integer, Integer>> listP = new ArrayList<Tuple2<Integer, Integer>>();
        System.out.println("[测试 cogroup函数]原始数据如下<2>:");
        listP.add(p1);
        listP.add(p2);
        listP.add(p3);
        listP.add(p4);

        listP.forEach(t -> {
            System.out.print(t._1 + "," + t._2() + " | ");
        });
        System.out.println("====================================");
        // 测试reduceByKey函数
        JavaPairRDD<Integer, Integer> rdd = JSC.parallelizePairs(list);
        JavaPairRDD<Integer, Integer> rddp = JSC.parallelizePairs(listP);

        List<Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>> result = rdd.cogroup(rddp).collect();
        System.out.println("[测试 cogroup函数]返回结果:" + result);
    }


    /**
     * lookUp 函数测试,就像使用 map的get方法一样，根据Key查询Value
     * [lookUp测试]返回结果:[3, 2, 4]
     */
    @Test
    public void lookUpTest() {
        Tuple2<Integer, Integer> t1 = new Tuple2<Integer, Integer>(1, 3);
        Tuple2<Integer, Integer> t2 = new Tuple2<Integer, Integer>(1, 2);
        Tuple2<Integer, Integer> t3 = new Tuple2<Integer, Integer>(1, 4);
        Tuple2<Integer, Integer> t4 = new Tuple2<Integer, Integer>(2, 3);
        Tuple2<Integer, Integer> t5 = new Tuple2<Integer, Integer>(2, 3);
        Tuple2<Integer, Integer> t6 = new Tuple2<Integer, Integer>(2, 5);
        List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
        System.out.println("[lookUp测试]原始数据如下:");
        list.add(t1);
        list.add(t2);
        list.add(t3);
        list.add(t4);
        list.add(t5);
        list.add(t6);

        list.forEach(t -> {
            System.out.println(t._1 + "," + t._2());
        });
        System.out.println("====================================");

        JavaPairRDD<Integer, Integer> rdd1 = JSC.parallelizePairs(list);

        List<Integer> result = rdd1.lookup(1);
        result.stream().forEach(System.out::println);
        System.out.println("[lookUp测试]返回结果:" + result);
    }

    /**
     * zip 拉链函数
     * [测试 zip 函数]返回结果:[(1,a), (2,b), (3,c), (4,d), (5,e), (6,f), (7,g), (8,h), (9,i), (10,j)]
     */
    @Test
    public void zipTest() {
        JavaRDD<Integer> rdd1 = JSC.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<String> rdd2 = JSC.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"));
        JavaPairRDD<Integer, String> result = rdd1.zip(rdd2);
        System.out.println("[测试 zip 函数]返回结果:" + result.collect());
    }

    /**
     * intersection 取得两个RDD的交集
     * [测试 zip 函数]返回结果:[(1,a), (2,b), (3,c), (4,d), (5,e), (6,f), (7,g), (8,h), (9,i), (10,j)]
     */
    @Test
    public void intersectionTest() {
        JavaRDD<Integer> rdd1 = JSC.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        JavaRDD<Integer> rdd2 = JSC.parallelize(Arrays.asList(6, 7, 8, 9, 10, 11));
        JavaRDD<Integer> result = rdd1.intersection(rdd2).sortBy(t -> t, false, 0);
        System.out.println("[测试 intersection 函数]返回结果:" + result.collect());
    }

    /**
     * keyBy
     * [测试 keyBy 函数]返回结果:[(3,dog), (6,salmon), (6,salmon), (3,rat), (8,elephant)]
     */
    @Test
    public void keyByTest() {
        JavaRDD<String> rdd1 = JSC.parallelize(Arrays.asList("dog", "salmon", "salmon", "rat", "elephant"));
        JavaPairRDD<Integer, String> result = rdd1.keyBy(t -> t.length()).sortByKey();
        System.out.println("[测试 keyBy 函数]返回结果:" + result.collect());

        JavaRDD<Integer> keysRdd = result.keys();
        System.out.println("[测试 keys 函数]返回结果:" + keysRdd.collect());
    }

}
