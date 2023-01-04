package com.song.hadoopdemo.sparkmllib;

import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 朴素贝叶斯 java版本
 */
public class NaiveBayesByJava {
    public static void main(String[] args) {
        //创建 SparkSession 对象
        SparkSession spark = SparkSession.builder().master("local[*]").appName("LinearRegressionWithSGD").getOrCreate();

        // 加载训练数据
        Dataset<Row> dataFrame = spark.read().format("libsvm").load("data/sample_libsvm_data.txt");
        // 将数据拆分为训练和测试
        Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.6, 0.4}, 1234L);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];

        // 创建训练并设置其参数
        NaiveBayes nb = new NaiveBayes();

        // 训练模型
        NaiveBayesModel model = nb.fit(train);

        //选择要显示的示例行 Select example rows to display.
        Dataset<Row> predictions = model.transform(test);
        predictions.show();

        // compute accuracy on the test set
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test set accuracy = " + accuracy);

    }
}
