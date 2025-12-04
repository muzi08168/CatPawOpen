import req from './req.js';
import CryptoJS from 'crypto-js';
import { PassThrough } from 'stream';

// --- 配置常量 ---
const DEBUG = true;
const DEFAULT_OPTIONS = {
    threads: 64,                    // 64 并发连接（更激进）
    chunkSize: 128 * 1024,          // 128KB 每块（更快响应）
    preloadChunks: 128,             // 预加载128块 = 16MB缓冲
    timeout: 15000,                 // 15秒超时
    retry: 3,
    minChunkSize: 32 * 1024,        // 首块最小32KB（快速首字节）
};

const log = (...args) => DEBUG && console.log('[Chunk]', new Date().toISOString().slice(11, 23), ...args);
const formatBytes = (bytes) => (bytes / 1024 / 1024).toFixed(2) + 'MB';
const formatSpeed = (bytesPerSec) => {
    if (bytesPerSec > 1024 * 1024) return (bytesPerSec / 1024 / 1024).toFixed(2) + ' MB/s';
    return (bytesPerSec / 1024).toFixed(1) + ' KB/s';
};

// 检测是否支持 Range
async function testSupport(url, headers) {
    try {
        const resp = await req.get(url, {
            responseType: 'stream',
            headers: { 'Range': 'bytes=0-0', ...headers },
            timeout: 10000
        });
        
        if (resp.status === 206) {
            const range = resp.headers['content-range'];
            const len = resp.headers['content-length'];
            const totalLength = range ? parseInt(range.split('/')[1]) : parseInt(len);
            
            const newHeaders = { ...resp.headers };
            delete newHeaders['content-range'];
            delete newHeaders['content-length'];
            if (totalLength) newHeaders['content-length'] = totalLength.toString();
            
            if (resp.data && resp.data.destroy) resp.data.destroy();
            
            return [true, newHeaders, totalLength];
        }
    } catch (err) {
        console.error('[Chunk] Support Test Error:', err.message);
    }
    return [false, null, 0];
}

const headerCache = new Map();

/**
 * 多连接并发流式下载
 * 核心策略：使用多个HTTP连接并行下载不同的块，突破单连接限速
 */
async function chunkStream(inReq, outResp, url, urlKey, headers, option) {
    const opt = { ...DEFAULT_OPTIONS, ...option };
    urlKey = urlKey || CryptoJS.enc.Hex.stringify(CryptoJS.MD5(url)).toString();
    
    // 获取文件元数据
    let meta = headerCache.get(urlKey);
    if (!meta) {
        log('Testing range support...');
        const [isSupport, h, total] = await testSupport(url, headers);
        if (!isSupport || !total) {
            log('Range not supported, redirecting');
            return outResp.redirect(url);
        }
        meta = { headers: h, totalLength: total };
        headerCache.set(urlKey, meta);
        log('Total size:', formatBytes(total));
        if (headerCache.size > 1000) headerCache.clear();
    }

    const { totalLength } = meta;

    // 处理客户端 Range 请求
    let startByte = 0;
    let endByte = totalLength - 1;
    let statusCode = 200;

    if (inReq.headers.range) {
        const ranges = inReq.headers.range.trim().split(/=|-/);
        startByte = parseInt(ranges[1]);
        if (ranges[2]) endByte = parseInt(ranges[2]);
        statusCode = 206;
    }

    const requestLength = endByte - startByte + 1;
    log(`Request: ${formatBytes(startByte)}-${formatBytes(endByte)} (${formatBytes(requestLength)})`);

    // 响应 Header
    const respHeaders = { ...meta.headers };
    respHeaders['content-length'] = requestLength.toString();
    respHeaders['content-range'] = `bytes ${startByte}-${endByte}/${totalLength}`;
    respHeaders['accept-ranges'] = 'bytes';
    
    outResp.code(statusCode);
    outResp.headers(respHeaders);

    // ============ 多连接并发下载核心逻辑 ============
    const chunkSize = opt.chunkSize;
    const totalChunks = Math.ceil(requestLength / chunkSize);
    const maxConcurrent = Math.min(opt.threads, totalChunks);
    
    log(`Chunks: ${totalChunks}, Concurrent: ${maxConcurrent}, ChunkSize: ${formatBytes(chunkSize)}`);

    // 块状态管理
    const chunks = new Map();  // chunkIndex -> { data, status }
    let nextChunkToSend = 0;
    let nextChunkToFetch = 0;
    let activeWorkers = 0;
    let totalSent = 0;
    let totalDownloaded = 0;
    const startTime = Date.now();
    
    const state = { 
        isDestroyed: false,
        error: null,
        pendingFetches: new Set()  // 正在下载的块索引
    };

    const stream = new PassThrough({ highWaterMark: 8 * 1024 * 1024 }); // 8MB 高水位

    // 计算块的字节范围
    const getChunkRange = (chunkIndex) => {
        const chunkStart = startByte + chunkIndex * chunkSize;
        const chunkEnd = Math.min(chunkStart + chunkSize - 1, endByte);
        return { start: chunkStart, end: chunkEnd };
    };

    // 下载单个块
    const fetchChunk = async (chunkIndex) => {
        if (state.isDestroyed || chunks.has(chunkIndex) || state.pendingFetches.has(chunkIndex)) {
            return false;
        }
        
        state.pendingFetches.add(chunkIndex);
        const { start, end } = getChunkRange(chunkIndex);
        let attempts = 0;
        
        while (attempts < opt.retry && !state.isDestroyed) {
            try {
                const res = await req.get(url, {
                    responseType: 'arraybuffer',
                    headers: { 
                        ...headers, 
                        'Range': `bytes=${start}-${end}`,
                        'Connection': 'keep-alive'
                    },
                    timeout: opt.timeout
                });
                
                if (res.data && !state.isDestroyed) {
                    chunks.set(chunkIndex, { data: Buffer.from(res.data), ready: true });
                    totalDownloaded += res.data.length;
                    state.pendingFetches.delete(chunkIndex);
                    return true;
                }
            } catch (e) {
                attempts++;
                if (attempts < opt.retry) {
                    await new Promise(r => setTimeout(r, 100 * attempts));
                }
            }
        }
        
        state.pendingFetches.delete(chunkIndex);
        if (!state.isDestroyed) {
            log(`Chunk ${chunkIndex} failed after ${attempts} attempts`);
        }
        return false;
    };

    // 获取下一个需要下载的块索引
    const getNextChunkToFetch = () => {
        // 从当前发送位置开始，找未下载且未在下载中的块
        for (let i = nextChunkToSend; i < totalChunks; i++) {
            if (!chunks.has(i) && !state.pendingFetches.has(i)) {
                return i;
            }
        }
        return -1;
    };

    // Worker：持续获取新块
    const worker = async (workerId) => {
        activeWorkers++;
        
        while (!state.isDestroyed) {
            const chunkToFetch = getNextChunkToFetch();
            
            // 没有更多块需要下载
            if (chunkToFetch === -1 || chunkToFetch >= totalChunks) {
                break;
            }
            
            // 限制预加载范围，避免下载太多后面的块
            if (chunkToFetch > nextChunkToSend + opt.preloadChunks) {
                await new Promise(r => setTimeout(r, 50));
                continue;
            }
            
            await fetchChunk(chunkToFetch);
        }
        
        activeWorkers--;
    };

    // 启动多个 Worker
    const startWorkers = () => {
        const neededWorkers = maxConcurrent - activeWorkers;
        for (let i = 0; i < neededWorkers && !state.isDestroyed; i++) {
            worker(activeWorkers + i);
        }
    };

    // 发送循环：按顺序发送已下载的块
    const sendLoop = async () => {
        let lastLogTime = Date.now();
        
        try {
            while (nextChunkToSend < totalChunks && !state.isDestroyed) {
                const chunk = chunks.get(nextChunkToSend);
                
                if (chunk && chunk.ready) {
                    // 发送块
                    const ok = stream.write(chunk.data);
                    totalSent += chunk.data.length;
                    
                    // 清理已发送的块，释放内存
                    chunks.delete(nextChunkToSend);
                    nextChunkToSend++;
                    
                    // 定期输出进度
                    const now = Date.now();
                    if (now - lastLogTime > 3000) {
                        const elapsed = (now - startTime) / 1000;
                        const speed = totalDownloaded / elapsed;
                        const progress = ((totalSent / requestLength) * 100).toFixed(1);
                        const bufferSize = chunks.size + state.pendingFetches.size;
                        log(`Progress: ${progress}%, Sent: ${formatBytes(totalSent)}, Speed: ${formatSpeed(speed)}, Workers: ${activeWorkers}, Buffer: ${bufferSize}`);
                        lastLogTime = now;
                    }
                    
                    // 背压处理
                    if (!ok) {
                        await new Promise(r => stream.once('drain', r));
                    }
                } else {
                    // 等待块下载完成，同时确保worker在运行
                    if (activeWorkers < maxConcurrent) {
                        startWorkers();
                    }
                    await new Promise(r => setTimeout(r, 5));
                }
            }
            
            // 完成
            const elapsed = (Date.now() - startTime) / 1000;
            const avgSpeed = totalSent / elapsed;
            log(`Complete: ${formatBytes(totalSent)} in ${elapsed.toFixed(1)}s, Avg: ${formatSpeed(avgSpeed)}`);
            stream.end();
            
        } catch (err) {
            console.error('[Chunk] Send Error:', err);
            state.error = err;
            stream.destroy(err);
        }
    };

    // 立即启动所有 worker
    log(`Starting ${maxConcurrent} workers...`);
    startWorkers();
    
    // 延迟一小段时间让worker开始下载，然后开始发送
    setTimeout(() => sendLoop(), 50);

    // 清理
    const cleanup = () => {
        if (state.isDestroyed) return;
        state.isDestroyed = true;
        chunks.clear();
        state.pendingFetches.clear();
        log('Stream closed, cleanup done');
    };

    stream.on('close', cleanup);
    stream.on('error', cleanup);
    if (outResp.raw?.on) {
        outResp.raw.on('close', cleanup);
    }

    return stream;
}

export default chunkStream;
