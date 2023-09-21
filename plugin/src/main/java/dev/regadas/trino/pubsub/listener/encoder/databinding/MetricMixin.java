package dev.regadas.trino.pubsub.listener.encoder.databinding;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NONE, property = "type")
public abstract class MetricMixin {}
