consumer = KafkaConsumer('data-ingestion', bootstrap_servers='localhost:9092', group_id='instances', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
producer = KafkaProducer('predictions', bootstrap_servers='localhost:9092', group_id='predictions', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
id = -2
for x, y, oblast in stream(consumer):
    if id >= max_instances:
        consumer.close()
        break
    else:
        id += 1
    if id >= 0:
        preds.loc[id, 'Oblast'] = oblast
        preds.loc[id, 'Target'] = y * totals[oblast]
    scaler.learn_one(x)
    x = scaler.transform_one(x)
    baseline.learn_one(x, y)
    if id >= 0:
        y_pred = baseline.predict_one(x)
        y_pred *= totals[oblast]
        preds.loc[id, 'baseline'] = y_pred
    x = np.array(list(x.values()))
    instance = Instance.from_array(schema, x)
    reg_instance = RegressionInstance.from_array(schema, x, y)
    if id >= 0:
        message = {}
        message['oblast'] = oblast
        message['id'] = id
        message['target'] = y
        message['predictions'] = {}
        message['predictions']['baseline'] = y_pred
    for model_name in model_names:
        if id >= 0:  # scikit-learn does not allows invoking predict in a model that was not fit before
            y_pred = models[model_name].predict(instance)
        models[model_name].train(reg_instance)
        if id >= 0:
            y_pred *= totals[oblast]
            y *= totals[oblast]
            evaluators[model_name].update(y, y_pred)
            preds.loc[id, model_name] = y_pred
            message['predictions'][model_name] = y_pred
    producer.send('predictions', value=message)
            
