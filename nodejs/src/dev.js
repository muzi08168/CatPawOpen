import { createServer } from 'http';
import {getWebsiteBundle, getDanmuBundle} from "../esbuild-website.js";

console.time('启动耗时')
globalThis.catServerFactory = (handle) => {
    let port = 0;
    const server = createServer((req, res) => {
        handle(req, res);
    });
    server.on('listening', () => {
        console.timeEnd('启动耗时')
        port = server.address().port;
        console.log('Run on ' + port);
    });
    server.on('close', () => {
        console.log('Close on ' + port);
    });
    return server;
};

globalThis.catDartServerPort = () => {
    return 0;
};

globalThis.DB_NAME = process.env.DB || 'default'

eval(await getWebsiteBundle());
eval(await getDanmuBundle());

import { start } from './index.js';

import * as config from './index.config.js';

start(config.default);
