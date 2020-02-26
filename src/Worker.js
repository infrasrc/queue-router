'use strict';

const EventEmitter = require('events');
const _ = require('lodash/fp');
const Joi = require('joi');
const Tracer = require('jaeger-tracer');
const { Tags, FORMAT_TEXT_MAP, globalTracer } = Tracer.opentracing;
const TWO_HOURS_IN_MS = 2 * 60 * 60 * 1000;

const messageStatuses = {
    processing: 'PROCESSING',
    proceed: 'PROCEED'
};

const timeout = (span, endTrace) => {
    span.timeout = setTimeout(() => {
        span.setTag("span.timeout", true);
        endTrace(span);
    }, TWO_HOURS_IN_MS);
    return span;
}

const parseJson = (jsonString) => {
    try {
        return JSON.parse(jsonString);
    }
    catch (error) {
        return jsonString;
    }
}

class Worker extends EventEmitter {
    constructor(consumer, router) {
        super();
        this._consumer = consumer;
        this.router = router;
        this.tracer = globalTracer();
    }

    get messageSchema() {
        return Joi.object({
            type: Joi.any().valid(_.values([...this.router.getRegisteredTypes()])).required(),
            content: Joi.any().required()
        }).required().unknown(false);
    }

    init() {
        this._consumer.on('error', error => this.emit('error', error));
        this._queue = this._consumer.createConsumer(this._handleMessage.bind(this));
        this._queue
            .on('error', error => this.emit('error', error))
            .on('message_error', error => this.emit('message_error', error))
            .on('message_received', info => this.emit('message_received', info))
            .on('message_processed', info => this.emit('message_processed', info))
            .on('stopped', info => this.emit('stopped', info))
            .on('idle', info => this.emit('idle', info));

        return this;
    }

    start() {
        this._queue.start();
    }

    stop() {
        this._queue.stop();
    }

    startTrace(traceId) {
        const carrier = {
            "uber-trace-id": traceId
        };
        let spanOptions = {
            tags: { [Tags.SPAN_KIND]: Tags.SPAN_KIND_MESSAGING_CONSUMER }
        }

        if (traceId) {
            spanOptions.childOf = this.tracer.extract(FORMAT_TEXT_MAP, carrier);
        }

        return timeout(this.tracer.startSpan('handleMessage', spanOptions), this.endTrace);
    }

    endTrace(span) {
        if (span) {
            if (span.finish instanceof Function) span.finish();
            if (span.timeout) clearTimeout(span.timeout);
        }
    }

    logEvent(span, event, value) {
        if ((!span) || (!span.log instanceof Function)) return;
        span.log({ event, value });
    }

    logError(span, errorObject, message, stack) {
        Tracer.logError(span, errorObject, message, stack);
    }

    setTag(span, tagName, tagValue){
        if ((!span) || (!span.log instanceof Function)) return;
        span.setTag(tagName, tagValue);
    }
    
    async _handleMessage(message, attributes) {
        let span = null;
        try {
            if (this.router.trace) {
                const traceId = _.getOr(null, 'traceId.StringValue')(attributes);
                span = this.startTrace(traceId);
            }

            this.setTag(span, "queue.address", this.consumer.queueUrl);

            const jsonMessage = parseJson(message);

            this.logEvent(span, 'handleMessage Request', { messageContent: jsonMessage, attributes })

            const jsonMessageValidationResult = this._validateMessage(jsonMessage, this.messageSchema);

            if (jsonMessageValidationResult.error) {
                throw jsonMessageValidationResult.error;
            };

            const controller = this.router.get(jsonMessage.type);
            const contentValidationResult = this._validateMessage(jsonMessage.content, controller.validation.schema);

            if (contentValidationResult.error) {
                throw contentValidationResult.error;
            };
 
            this.emit('message', { type: jsonMessage.type, status: messageStatuses.processing });

            const result = await controller.handler(contentValidationResult.value, attributes, span);

            if(!_.isEmpty(result)) this.logEvent(span, 'handler result', result);

            this.endTrace(span);
            this.emit('message', { type: jsonMessage.type, status: messageStatuses.proceed });

        } catch (error) {
            this.logError(span, error, error.message, error.stack);
            delete error.traced;    
            this.endTrace(span);
            this.emit('error', error);
        }
    }

    _validateMessage(message, schema) {
        const validationResult = Joi.validate(message, schema);
        if (validationResult.error) {
            this.emit('error', new Error(`Invalid message, error: ${validationResult.error}, message: ${JSON.stringify(message)}`));
        }

        return validationResult;
    }
}

module.exports = Worker;
