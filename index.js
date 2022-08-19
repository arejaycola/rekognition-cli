const { Rekognition } = require('aws-sdk');
const fs = require('fs');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');
const argv = yargs(hideBin(process.argv)).argv;


const client = new Rekognition({ region: 'us-east-1' });


const detect = async () => {
	const params = {
		Image: {
			Bytes: fs.readFileSync(argv.image),
		},
	};

	client.detectLabels(params, (err, data) => {
		console.log(data);
	});
};

detect();
