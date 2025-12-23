import fastify from 'fastify';
import router from './router.js';
import { JsonDB, Config } from 'node-json-db';
import axios from 'axios';
import website from "./website/index.js";
import {getIPAddress} from "./util/network.js";
import {getCache as getPansCache} from "./website/pans.js";

let server = null;

/**
 * Start the server with the given configuration.
 *
 * Be careful that start will be called multiple times when
 * work with catvodapp. If the server is already running,
 * the stop will be called by engine before start, make sure
 * to return new server every time.
 *
 * @param {Map} config - the config of the server
 * @return {void}
 */
export async function start(config) {
    /**
     * @type {import('fastify').FastifyInstance}
     */
    server = fastify({
        serverFactory: catServerFactory,
        forceCloseConnections: true,
        logger: !!(process.env.NODE_ENV !== 'development'),
        maxParamLength: 10240,
    });
    globalThis.messageToDart = server.messageToDart = async (data) => {
        try {
            console.log('messageToDart', data);
            const port = catDartServerPort();
            if (port == 0) {
                return null;
            }
            const resp = await axios.post(`http://127.0.0.1:${port}/msg`, data);
            return resp.data;
        } catch (error) {
            console.error(error);
            return null;
        }
    };
    server.address = function () {
        const result = this.server.address();
        result.url = `http://${getIPAddress()}:${result.port}`;
        result.dynamic = 'js2p://_WEB_';
        return result;
    };
    server.addHook('onError', async (_request, _reply, error) => {
        console.error(error);
        if (!error.statusCode) error.statusCode = 500;
        return error;
    });
    server.stop = false;
    server.config = config;
    // 推荐使用NODE_PATH做db存储的更目录，这个目录在应用中清除缓存时会被清空
    server.db = new JsonDB(new Config((process.env['NODE_PATH'] || '.') + `/${DB_NAME}.db.json`, true, true, '/', true));
    let oldPush = server.db.push.bind(server.db);
    server.db.push = async (...args) => {
        let rs = await oldPush(...args);
        // 异步存储配置
        server.db.getData("/")
          .then(allData => {
              server.messageToDart({
                  action: 'saveProfile',
                  opt: allData,
              });
          })
        return rs
    }
    let oldDelete = server.db.delete.bind(server.db);
    server.db.delete = async (...args) => {
        let rs = await oldDelete(...args);
        // 异步存储配置
        server.db.getData("/")
          .then(allData => {
              server.messageToDart({
                  action: 'saveProfile',
                  opt: allData,
              });
          })
        return rs
    }
    // 异步恢复配置
    server.messageToDart({
        action: 'queryProfile'
    })
      .then(allData => {
          if (allData && Object.keys(allData).length > 0) {
              oldPush('/', allData || {})
          }
      });
    server.register(router, {db: server.db, config});
    server.register(website, { prefix: '/website' });
    globalThis.Pans = await getPansCache(server)
    globalThis.getPanName = (key) => globalThis.Pans.find(pan => pan.key === key)?.name
    globalThis.getPanEnabled = (key) => globalThis.Pans.find(pan => pan.key === key)?.enable
    const startServer = (port) => {
        server.listen({ port: process.env['DEV_HTTP_PORT'] || port, host: '0.0.0.0' }, (err, address) => {
            if (err) {
                console.error(err);
                if (err.code === 'EADDRINUSE') {
                    console.log(`Port ${port} is already in use. Trying next available port...`);
                    startServer(port + 1);
                }
            } else {
                console.log(`Server listening on ${address}`);
            }
        });
    }
    startServer(9988)
}

/**
 * Stop the server if it exists.
 *
 */
export async function stop() {
    if (server) {
        server.close();
        server.stop = true;
    }
    server = null;
}
