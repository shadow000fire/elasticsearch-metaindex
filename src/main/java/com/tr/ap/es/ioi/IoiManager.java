package com.tr.ap.es.ioi;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;

public class IoiManager extends AbstractComponent implements ClusterStateListener
{
    protected final ESLogger   logger;
    private Client             client;
    private Collection<String> indexList;
    private ClusterService     clusterService;
    private MasterListener     masterListener;

    @Inject
    public IoiManager(Settings settings, Client client, ClusterService clusterService)
    {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        logger = Loggers.getLogger(getClass());
        indexList = new HashSet<String>(100);
        masterListener = new MasterListener();

        clusterService.add(masterListener);
    }

    private void initialize(ClusterState state)
    {
        String indices="";
        for(ObjectCursor<String> cur: state.getMetaData().indices().keys())
        {
            indices += cur.value+", ";
        }
        logger.info("Initializing with indices: {}", indices);
        
        indexList.clear();
        if (state.getMetaData().hasIndex(Names.INDEX))
        {
            logger.info("ioi index already exists");
            // if ioi already existed and we just became master, assume its
            // already populated
            for (ObjectCursor<String> cursor : state.getMetaData().indices().keys())
            {
                indexList.add(cursor.value);
            }
        }
        else
        {
            logger.info("Settings up ioi index");
            try
            {
                CreateIndexRequestBuilder request = client.admin().indices().prepareCreate(Names.INDEX);
                ImmutableSettings.Builder indexSettings = ImmutableSettings.builder().put("number_of_shards", 1)
                        .put("number_of_replicas", 0);
                request.setSettings(indexSettings);

                // default field settings
                XContentBuilder defaultBuilder = XContentFactory.jsonBuilder().startObject();
                defaultBuilder.field("numeric_detection", true);
                defaultBuilder.startArray("dynamic_templates").startObject().startObject("strings").field("match", "*")
                        .field("match_mapping_type", "string").startObject("mapping").field("type", "string")
                        .field("index", "not_analyzed").endObject().endObject().endObject().endArray().endObject();
                request.addMapping("_default_", defaultBuilder);

                // index type
                XContentBuilder indexBuilder = XContentFactory.jsonBuilder().startObject();
                indexBuilder.startObject("properties");
                indexBuilder.startObject(Names.FIELD_FIELD).startObject("properties");
                indexBuilder.startObject(Names.FIELD_FIELD_NAME).field("type", "string").field("analyzer", "english")
                        .endObject();
                indexBuilder.endObject().endObject().endObject().endObject();
                request.addMapping(Names.TYPE_INDEX, indexBuilder);

                if (!request.get().isAcknowledged())
                {
                    logger.warn("Attempt to create index wasn't acknowledged");
                }
            }
            catch (Exception e)
            {
                logger.error("Could not create mapping", e);
            }

            BulkRequestBuilder bulk = client.prepareBulk();
            for (ObjectCursor<IndexMetaData> cursor : state.getMetaData().indices().values())
            {
                insertMeta(cursor.value,bulk);
            }

            if (bulk.numberOfActions() > 0)
            {
                bulk.get();
            }
        }
    }

    private void clear()
    {
        indexList.clear();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event)
    {
        logger.info("Processing cluster changed event");
        BulkRequestBuilder bulk = client.prepareBulk();
        for (String index : event.indicesCreated())
        {
            logger.info("Detected new index {}", index);
            IndexMetaData meta = event.state().getMetaData().index(index);
            insertMeta(meta, bulk);
        }

        for (String index : event.indicesDeleted())
        {
            logger.info("Detected deleted index {}", index);
            bulk.add(deleteMeta(index));
        }

        if (event.metaDataChanged())
        {
            logger.info("Searching for changed indices");
            Iterator<IndexMetaData> oldIter = event.previousState().getMetaData().iterator();
            IndexMetaData oldMeta;
            while (oldIter.hasNext())
            {
                oldMeta = oldIter.next();
                if (event.indexMetaDataChanged(oldMeta))
                {
                    insertMeta(event.state().metaData().index(oldMeta.getIndex()),bulk);
                }
            }
        }

        if (bulk.numberOfActions() > 0)
        {
            try
            {
                bulk.get();
            }
            catch (ElasticsearchException e)
            {
                logger.error("Error sending bulk", e);
            }
        }
    }
    
    private XContentBuilder createNewDoc()
    {
        try
        {
            return XContentBuilder.builder(XContentType.JSON.xContent());
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return null;
        }
    }

    private void insertMeta(IndexMetaData meta, BulkRequestBuilder bulk)
    {
        try
        {
            Iterator<ObjectObjectCursor<String, MappingMetaData>> iter = meta.getMappings().iterator();
            ObjectObjectCursor<String, MappingMetaData> cursor;
            while (iter.hasNext())
            {
                cursor = iter.next();
                logger.info("Found mapping: {}", cursor.key);
                addMapping(bulk, cursor.value.getSourceAsMap(), meta.getIndex(), cursor.key);
            }
        }
        catch (IOException e)
        {
            logger.error("error creating document", e);
        }
    }

    private void addMapping(BulkRequestBuilder bulk, Map<String, Object> map, String indexName, String typeName) throws IOException
    {
        IndexRequestBuilder request = client.prepareIndex(Names.INDEX, Names.TYPE_INDEX, indexName);
        XContentBuilder builder = createNewDoc().startObject();
        builder.startArray("mapping");
        builder.startObject();
        builder.field("name", typeName);
        for (Entry<String, Object> entry : map.entrySet())
        {
            logger.info("Found entry {}", entry.getKey());
            if ("properties".equals(entry.getKey()))
            {
                logger.info("Handling as known 'properties'");
                builder.startArray(Names.FIELD_FIELD);
                addDataFields(builder, (Map) entry.getValue(), typeName, "");
                builder.endArray();
            }
            else if (entry.getValue() instanceof List)
            {
                logger.info("Handling as List");
                builder.startObject(Names.FIELD_CONFIG);
                addGenericFields(builder, (List) entry.getValue(), entry.getKey());
                builder.endObject();
            }
            else if (entry.getValue() instanceof Map)
            {
                logger.info("Handling as Map");
                builder.startObject(Names.FIELD_CONFIG);
                addGenericFields(builder, (Map) entry.getValue(), entry.getKey(), Collections.<String>emptyList());
                builder.endObject();
            }
            else
            {
                logger.info("Not sure how to handle mapping entry:{} - {}", entry.getClass(), entry);
            }
        }
        builder.endObject();
        builder.endArray().endObject();
        request.setSource(builder);
        logger.info("Doc: {}", builder.string());
        bulk.add(request);
    }

    private void addDataFields(XContentBuilder builder, Map<String, Object> map, String typeName, String prefix)
            throws IOException
    {
        // each entry is a field definition, possible an inner-object
        Map<String, Object> fieldDefinition;
        String appliedPrefix = prefix.isEmpty() ? "" : prefix + ".";
        for (Entry<String, Object> entry : map.entrySet())
        {
            logger.info("Found data field: {}", entry.getKey());
            fieldDefinition = (Map<String, Object>) entry.getValue();
            if (fieldDefinition.containsKey("properties"))
            {
                logger.info("recursing on object data field");
                addDataFields(builder, (Map) fieldDefinition.get("properties"), typeName,
                        appliedPrefix + entry.getKey());
            }
            else
            {
                logger.info("Handled");
                builder.startObject();
                builder.field(Names.FIELD_FIELD_NAME, appliedPrefix + entry.getKey());
                builder.field(Names.FIELD_FIELD_RAW, entry.getKey());
                addGenericFields(builder, fieldDefinition, "", Collections.<String>emptyList());
                builder.endObject();
            }
        }
    }

    private void addGenericFields(XContentBuilder builder, Map<String, Object> map, String prefix, Collection<String> exclude) throws IOException
    {
        String appliedPrefix = prefix.isEmpty() ? "" : prefix + ".";
        logger.info("Iterating through Map");
        for (Entry<String, Object> entry : map.entrySet())
        {
            if(exclude.contains(entry.getKey()))
            {
                continue;
            }
            
            logger.info("Found generic field: {}", entry.getKey());
            if (entry.getValue() instanceof Map)
            {
                logger.info("recursing on generic Map field");
                addGenericFields(builder, (Map) entry.getValue(), appliedPrefix + entry.getKey(), exclude);
            }
            else if (entry instanceof List)
            {
                logger.info("recursing on generic List field");
                addGenericFields(builder, (List) entry, prefix);
            }
            else
            {
                logger.info("Handled");
                builder.field(appliedPrefix + entry.getKey(), entry.getValue());
            }
        }
    }

    private void addGenericFields(XContentBuilder builder, List<Object> list, String prefix) throws IOException
    {
        logger.info("Iterating through List");
        for (Object entry : list)
        {
            if (entry instanceof Map)
            {
                logger.info("recursing on generic Map field");
                addGenericFields(builder, (Map) entry, prefix, Collections.<String>emptyList());
            }
            else if (entry instanceof List)
            {
                logger.info("recursing on generic List field");
                addGenericFields(builder, (List) entry, prefix);
            }
            else
            {
                logger.info("Not sure how to handle generic field:{} - {}", entry.getClass(), entry);
            }
        }
    }

    private DeleteRequestBuilder deleteMeta(String index)
    {
        indexList.remove(index);
        return client.prepareDelete(Names.INDEX, Names.TYPE_INDEX, index);
    }

    private class MasterListener implements LocalNodeMasterListener
    {
        public MasterListener()
        {
        }

        @Override
        public void onMaster()
        {
            logger.info("onMaster()");
            IoiManager.this.initialize(clusterService.state());
            clusterService.add(IoiManager.this);
        }

        @Override
        public void offMaster()
        {
            logger.info("offMaster()");
            clusterService.remove(IoiManager.this);
            IoiManager.this.clear();
        }

        @Override
        public String executorName()
        {
            return ThreadPool.Names.GENERIC;
        }

    }

    private static class Names
    {
        public static final String INDEX            = "ioi";
        public static final String TYPE_INDEX       = "index";
        public static final String FIELD_FIELD      = "field";
        public static final String FIELD_CONFIG     = "configuration";
        public static final String FIELD_FIELD_RAW  = "raw";
        public static final String FIELD_FIELD_NAME = "name";
    }
}
