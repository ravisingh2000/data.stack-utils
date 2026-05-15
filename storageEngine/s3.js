const AWS = require('aws-sdk');
const fs = require("fs");
const log4js = require('log4js');

const logLevel = process.env.LOG_LEVEL ? process.env.LOG_LEVEL : 'info';
log4js.configure({
    levels: {
        AUDIT: { value: Number.MAX_VALUE - 1, colour: 'yellow' }
    },
    appenders: { out: { type: 'stdout', layout: { type: 'basic' } } },
    categories: { default: { appenders: ['out'], level: logLevel.toUpperCase() } }
});
let version = require('../package.json').version;
const loggerName = process.env.HOSTNAME ? `[${process.env.DATA_STACK_NAMESPACE}] [${process.env.HOSTNAME}] [STORAGE_ENGINE ${version}]` : `[STORAGE_ENGINE ${version}]`;
let logger = log4js.getLogger(loggerName);


let e = {};


e.uploadFile = async (data) => {
    try {
        logger.debug(`Uploading file to S3 Bucket : ${data.file.metadata.fileName}`);
        logger.debug(JSON.stringify({ region: data.region, bucket: data.bucket }));

        AWS.config.update({
            accessKeyId: data.accessKeyId,
            secretAccessKey: data.secretAccessKey,
            region: data.region
        });

        let s3 = new AWS.S3();

        let uploadParams = {
            Bucket: data.bucket,
            Key: data.file.fileName,
            Metadata: {
                'data_stack_filename': data.file.metadata.filename,
                'data_stack_app': data.appName,
                'data_stack_dataServiceId': data.serviceId,
                'data_stack_dataServiceName': data.serviceName
            }
        };

        let stream = fs.createReadStream(data.file.path);
        stream.on('error', (err) => {
            logger.error('Error streaming file :: ', err)
        });

        uploadParams.Body = stream;

        let uploadedFile = await s3.upload(uploadParams).promise();

        logger.info(`Upload Success :: ${JSON.stringify(uploadedFile.Location)}`);
    } catch (err) {
        logger.error(`Error uploading file to S3 :: ${err}`);
    }
};


e.downloadFileBuffer = async (data) => {
    try {
        logger.debug(`Downloading File from S3 :: ${data.fileName}`);
        logger.trace(JSON.stringify({ region: data.region, bucket: data.bucket }));

        AWS.config.update({
            accessKeyId: data.accessKeyId,
            secretAccessKey: data.secretAccessKey,
            region: data.region
        });

        let s3 = new AWS.S3();

        let readParams = {
            Bucket: data.bucket,
            Key: data.fileName
        };

        let readStream = s3.getObject(readParams).createReadStream();

        return new Promise((resolve, reject) => {
            let chunks = [];
            readStream.on('data', (data) => {
                chunks.push(data);
            });
            readStream.on('end', () => {
                var chunk = Buffer.concat(chunks);
                logger.debug(`Downloaded buffer from S3 :: ${data.fileName}`);
                resolve(chunk);
            });
            readStream.on('error', (err) => {
                logger.error(`Error downloading file from S3 :: ${data.fileName} :: ${err.message}`);
                reject(err);
            })
        });
    } catch (err) {
        logger.error(`Error downloading file as buffer from S3 :: ${data.fileName} :: ${err.message}`);
        throw new Error(err);
    }
};


e.deleteFile = async (data) => {
    try {
        logger.debug(`Deleting File from S3 :: ${data.fileName}`);
        logger.trace(JSON.stringify({ region: data.region, bucket: data.bucket }));

        AWS.config.update({
            accessKeyId: data.accessKeyId,
            secretAccessKey: data.secretAccessKey,
            region: data.region
        });

        let s3 = new AWS.S3();

        let deleteParams = {
            Bucket: data.bucket,
            Key: data.fileName
        };

        await s3.deleteObject(deleteParams).promise();
        logger.info(`File deleted in S3`);

    } catch (err) {
        logger.error(`Error deleting file from S3 :: ${data.fileName} :: ${err.message}`);
        throw new Error(err);
    }
};


module.exports = e;
