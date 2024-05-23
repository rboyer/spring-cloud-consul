/*
 * Copyright 2013-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.consul.discovery;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.catalog.CatalogServicesRequest;
import io.micrometer.core.annotation.Timed;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * @author Spencer Gibb
 */
public class ConsulCatalogWatch implements ApplicationEventPublisherAware, SmartLifecycle {

	private static final Log log = LogFactory.getLog(ConsulCatalogWatch.class);

	private final ConsulDiscoveryProperties properties;

	private final ConsulClient consul;

	private final TaskScheduler taskScheduler;

	private final AtomicReference<BigInteger> catalogServicesIndex = new AtomicReference<>();

	private final AtomicBoolean running = new AtomicBoolean(false);

	private ApplicationEventPublisher publisher;

	private final ExponentialBackOff backOff = new ExponentialBackOff(500L, 1.5);

	private LongLivedExponentialBackOffTrigger watchTrigger;

	private ScheduledFuture<?> watchFuture;

	public ConsulCatalogWatch(ConsulDiscoveryProperties properties, ConsulClient consul) {
		this(properties, consul, getTaskScheduler());
	}

	public ConsulCatalogWatch(ConsulDiscoveryProperties properties, ConsulClient consul, TaskScheduler taskScheduler) {
		this.properties = properties;
		this.consul = consul;
		this.taskScheduler = taskScheduler;
	}

	private static ThreadPoolTaskScheduler getTaskScheduler() {
		ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
		taskScheduler.initialize();
		return taskScheduler;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher publisher) {
		this.publisher = publisher;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		this.stop();
		callback.run();
	}

	@Override
	public void start() {
		if (this.running.compareAndSet(false, true)) {
			this.watchTrigger = new LongLivedExponentialBackOffTrigger(backOff);
			this.watchFuture = this.taskScheduler.schedule(this::catalogServicesWatch, this.watchTrigger);
		}
	}

	@Override
	public void stop() {
		if (this.running.compareAndSet(true, false) && this.watchFuture != null) {
			this.watchFuture.cancel(true);
		}
	}

	@Override
	public boolean isRunning() {
		return this.running.get();
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Timed("consul.watch-catalog-services")
	public void catalogServicesWatch() {
		watchTrigger.markStart();
		boolean success = false;
		try {
			long index = -1;
			if (this.catalogServicesIndex.get() != null) {
				index = this.catalogServicesIndex.get().longValue();
			}

			QueryParams queryParams = properties.queryParamsBuilder().setIndex(index).build();

			CatalogServicesRequest request = CatalogServicesRequest.newBuilder().setQueryParams(queryParams)
					.setToken(this.properties.getAclToken()).build();
			Response<Map<String, List<String>>> response = this.consul.getCatalogServices(request);
			Long consulIndex = response.getConsulIndex();
			if (consulIndex != null) {
				this.catalogServicesIndex.set(BigInteger.valueOf(consulIndex));
			}

			if (log.isTraceEnabled()) {
				log.trace("Received services update from consul: " + response.getValue() + ", index: " + consulIndex);
			}
			this.publisher.publishEvent(new HeartbeatEvent(this, consulIndex));
			success = true;
		}
		catch (Exception e) {
			log.error("Error watching Consul CatalogServices", e);
		}
		finally {
			if (success) {
				watchTrigger.markSuccess();
			}
			else {
				watchTrigger.markFailure();
			}
		}
	}

	private static final class LongLivedExponentialBackOffTrigger implements Trigger {

		private final ExponentialBackOff backOff;

		BackOffExecution exec = null;

		boolean lastRunFailed = false;

		LongLivedExponentialBackOffTrigger(ExponentialBackOff backOff) {
			this.backOff = backOff;
		}

		void markStart() {
			// Reset the exponential clock on each successful start.
			if (!lastRunFailed || this.exec == null) {
				this.exec = backOff.start();
			}
		}

		void markSuccess() {
			lastRunFailed = false;
		}

		void markFailure() {
			lastRunFailed = true;
		}

		@Override
		public Instant nextExecution(TriggerContext triggerContext) {
			Duration duration = nextDuration();
			if (duration == null) {
				return null;
			}

			if (duration.isZero()) {
				return Instant.now(); // first time
			}

			return Instant.now().plus(duration);
		}

		private Duration nextDuration() {
			if (!lastRunFailed) {
				if (exec == null) {
					return Duration.ZERO; // first time
				}
				// Linear backoff between successful calls.
				return Duration.ofMillis(backOff.getInitialInterval());
			}
			long waitTimeout = exec.nextBackOff();
			if (waitTimeout == BackOffExecution.STOP) {
				return null;
			}
			return Duration.ofMillis(waitTimeout);
		}
	}
}
