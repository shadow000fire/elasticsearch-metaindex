package com.tr.ap.es.ioi;

import java.util.Collection;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;

public class IoiPlugin extends AbstractPlugin
{
    public IoiPlugin()
    {
    }

    @Override
    public String name()
    {
        return "IoI";
    }

    @Override
    public String description()
    {
        return "Index of Indices plugin";
    }

    @Override
    public Collection<Class<? extends Module>> modules()
    {
        return ImmutableList.<Class<? extends Module>> of(IoiModule.class);
    }

}
