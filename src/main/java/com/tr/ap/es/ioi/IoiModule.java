package com.tr.ap.es.ioi;

import org.elasticsearch.common.inject.AbstractModule;

public class IoiModule extends AbstractModule
{

    @Override
    protected void configure()
    {
        bind(IoiManager.class).asEagerSingleton();
    }

}
