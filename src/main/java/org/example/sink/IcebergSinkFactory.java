package org.example.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.example.config.AppConfig;

import java.util.Collections;

/**
 * Iceberg Sink 工厂：创建 FlinkSink，支持 checkpoint 感知、exactly-once、自动文件滚动。
 * 通过 Glue Catalog + S3 写入。
 */
public final class IcebergSinkFactory {

    private IcebergSinkFactory() {}

    /**
     * 创建 Iceberg FlinkSink（append 模式）
     */
    public static void create(DataStream<RowData> inputStream) {
        CatalogLoader catalogLoader = CatalogLoader.custom(
                AppConfig.ICEBERG_CATALOG_NAME,
                Collections.singletonMap("warehouse", AppConfig.ICEBERG_WAREHOUSE),
                new org.apache.hadoop.conf.Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog"
        );

        TableLoader tableLoader = TableLoader.fromCatalog(
                catalogLoader,
                TableIdentifier.of(AppConfig.ICEBERG_DATABASE, AppConfig.ICEBERG_TABLE)
        );

        FlinkSink.builderFor(inputStream, r -> r, TypeInformation.of(RowData.class))
                .tableLoader(tableLoader)
                .append();
    }
}
