import { IOS_UA, isEmpty} from './misc.js';
import * as Ali from './ali.js';
import * as Quark from './quark.js';
import * as UC from './uc.js';
import {Cloud, initCloud} from "./cloud.js";
import {Yun} from "./yun.js";
import {initPan123Cloud, Pan} from "./pan123.js";
import * as Y115 from './115.js';
import * as Baidu from './baidu.js';
import {videosHandle} from "./utils.js";
import {is115Link, is123Link, isAliLink, isBaiduLink, isQuarkLink, isTyLink, isUcLink, isYdLink} from "./linkDetect.js";

export { isEmpty };
export const ua = IOS_UA;

export async function init(inReq, _outResp) {
    return {};
}

export async function detail(shareUrls, req) {
    shareUrls = !Array.isArray(shareUrls) ? [shareUrls] : shareUrls;
    const shareUrlsWithMoreInfo = shareUrls.map(url => {
        const key = getPanInfos().find(pan => pan.validator(url))?.key
        return {
            key,
            url,
            order: globalThis.Pans.findIndex(pan => pan.key === key)
        }
    }).filter(item => item.key)
    shareUrlsWithMoreInfo.sort((a, b) => a.order - b.order);
    shareUrls = shareUrlsWithMoreInfo.map(item => item.url)

    const froms = [];
    const urls = [];
    for (const shareUrl of shareUrls) {
        if (isAliLink(shareUrl)) {
            const data = await Ali.detail(shareUrl);
            if(data && data.from && data.url){
                froms.push(data.from);
                urls.push(data.url);
            }
        } else if (isQuarkLink(shareUrl)) {
            const data = await Quark.detail(shareUrl);
            if(data && data.from && data.url){
                froms.push(data.from);
                urls.push(data.url);
            }
        } else if (isUcLink(shareUrl)) {
            const data = await UC.detail(shareUrl);
            if(data && data.from && data.url){
                froms.push(data.from);
                urls.push(data.url);
            }
        } else if (isTyLink(shareUrl)) {
            const shareData = await Cloud.getShareData(shareUrl);
            if(shareData) {
                const panName = await getPanName('tianyi')
                Object.keys(shareData).forEach(it => {
                    const data = videosHandle(panName + '-' + it, shareData[it])
                    if(data && data.from && data.url){
                        froms.push(data.from);
                        urls.push(data.url);
                    }
                })
            }
        } else if (isYdLink(shareUrl)) {
            let shareData = await Yun.getShareData(shareUrl)
            const panName = await getPanName('yidong')
            Object.keys(shareData).forEach(it => {
                const data = videosHandle(panName + '-' + it, shareData[it])
                if(data && data.from && data.url){
                    froms.push(data.from);
                    urls.push(data.url);
                }
            })
        } else if(is123Link(shareUrl)) {
            const shareData = await Pan.getShareData(shareUrl)
            let files = await Pan.getFilesByShareUrl(shareData)
            const panName = await getPanName('123')
            Object.keys(files).forEach(it => {
                const data = videosHandle(panName + '-' + it, files[it])
                if(data && data.from && data.url){
                    froms.push(data.from);
                    urls.push(data.url);
                }
            })
        } else if(is115Link(shareUrl)) {
            const data = await Y115.detail(shareUrl);
            if(data && data.from && data.url){
                froms.push(data.from);
                urls.push(data.url);
            }
        } else if(isBaiduLink(shareUrl)) {
            const data = await Baidu.detail(shareUrl, req);
            if(data && data.from && data.url){
                froms.push(data.from);
                urls.push(data.url);
            }
        }
    }
    // 避免同名线路
    const fromsVisitedMap = {}
    froms.forEach((item, index) => {
        if (!fromsVisitedMap[item]) {
            fromsVisitedMap[item] = 1
        } else {
            froms[index] = `${item}-${++fromsVisitedMap[item]}`
        }
    })

    return {
        froms: froms.join('$$$'),
        urls: urls.join('$$$')
    };
}

export async function proxy(inReq, _outResp) {
    const site = inReq.params.site;
    if (site == 'ali') {
        return await Ali.proxy(inReq, _outResp);
    } else if (site == 'quark') {
        return await Quark.proxy(inReq, _outResp);
    } else if (site == 'uc') {
        return await UC.proxy(inReq, _outResp);
    } else if (site == 'baidu') {
        return await Baidu.proxy(inReq, _outResp);
    }
}

export async function play(inReq, _outResp) {
    const flag = inReq.body.flag;
    if (flag.startsWith(await getPanName('ali'))) {
        return await Ali.play(inReq, _outResp);
    } else if (flag.startsWith(await getPanName('quark'))) {
        return await Quark.play(inReq, _outResp);
    } else if (flag.startsWith(await getPanName('uc'))) {
        return await UC.play(inReq, _outResp);
    } else if (flag.startsWith(await getPanName('tianyi'))) {
        const ids = inReq.body.id.split('*');
        await initCloud(inReq)
        const url = await Cloud.getShareUrl(ids[0], ids[1]);
        return {
            parse: 0,
            url: ['原画', url],
        }
    } else if (flag.startsWith(await getPanName('yidong'))) {
        const ids = inReq.body.id.split('*');
        const url = await Yun.getSharePlay(ids[0], ids[1]);
        return {
            parse: 0,
            url: ['原画', url],
        }
    } else if (flag.startsWith(await getPanName('123'))) {
        await initPan123Cloud(inReq)
        const ids = inReq.body.id.split('*');
        const url = await Pan.getDownload(...ids);
        return {
            parse: 0,
            url: ['原画', url],
        }
    } else if (flag.startsWith(await getPanName('115'))) {
        return await Y115.play(inReq, _outResp);
    } else if (flag.startsWith(await getPanName('baidu'))) {
        return await Baidu.play(inReq, _outResp);
    }
}

export async function test(inReq, outResp) {
    try {
        const prefix = inReq.server.prefix;
        const dataResult = {};
        let resp = await inReq.server.inject().post(`${prefix}/init`);
        dataResult.init = resp.json();
        printErr(resp.json());
        resp = await inReq.server.inject().post(`${prefix}/home`);
        dataResult.home = resp.json();
        printErr(resp.json());
        let detailCalled = false;
        if (dataResult.home.class && dataResult.home.class.length > 0) {
            const typeId = dataResult.home.class[0].type_id;
            let filters = {};
            if (dataResult.home.filters) {
                let filter = dataResult.home.filters[typeId];
                if (filter) {
                    for (const filterCfg of filter) {
                        const initValue = filterCfg.init;
                        if (!initValue) continue;
                        for (const value of filterCfg.value) {
                            if (value.v == initValue) {
                                filters[filterCfg.key] = initValue;
                                break;
                            }
                        }
                    }
                }
            }
            resp = await inReq.server.inject().post(`${prefix}/category`).payload({
                id: typeId,
                page: 1,
                filter: true,
                filters: filters,
            });
            dataResult.category = resp.json();
            printErr(resp.json());
            if (dataResult.category.list.length > 0) {
                detailCalled = true;
                const vodId = dataResult.category.list[0].vod_id;
                await detailTest(inReq, vodId, dataResult);
            }
        }
        resp = await inReq.server.inject().post(`${prefix}/search`).payload({
            wd: '仙逆',
            page: 1,
        });
        dataResult.search = resp.json();
        if (!detailCalled && dataResult.search.list.length > 0) {
            const vodId = dataResult.search.list[0].vod_id;
            await detailTest(inReq, vodId, dataResult);
        }
        printErr(resp.json());
        return dataResult;
    } catch (err) {
        console.error(err);
        outResp.code(500);
        return { err: err.message, tip: 'check debug console output' };
    }
}

async function detailTest(inReq, vodId, dataResult) {
    const prefix = inReq.server.prefix;
    let resp = await inReq.server.inject().post(`${prefix}/detail`).payload({
        id: vodId,
    });
    dataResult.detail = resp.json();
    printErr(resp.json());
    if (dataResult.detail.list && dataResult.detail.list.length > 0) {
        dataResult.play = [];
        for (const vod of dataResult.detail.list) {
            const flags = vod.vod_play_from.split('$$$');
            const ids = vod.vod_play_url.split('$$$');
            for (let j = 0; j < flags.length; j++) {
                const flag = flags[j];
                const urls = ids[j].split('#');
                for (let i = 0; i < urls.length && i < 2; i++) {
                    resp = await inReq.server
                        .inject()
                        .post(`${prefix}/play`)
                        .payload({
                            flag: flag,
                            id: urls[i].split('$')[1],
                        });
                    dataResult.play.push(resp.json());
                }
            }
        }
    }
}

function printErr(json) {
    if (json.statusCode && json.statusCode == 500) {
        console.error(json);
    }
}

export const getPanInfos = () =>  [
    {key: 'yidong', name: getPanName('yidong'), validator: isYdLink, 'pic': 'https://yun.139.com/w/static/img/LOGO.png'},
    {key: 'tianyi', name: getPanName('tianyi'), validator: isTyLink, pic: 'https://is1-ssl.mzstatic.com/image/thumb/Purple211/v4/a8/fa/f0/a8faf032-0fa4-d9c5-ac70-920d9c84dff1/AppIcon-0-0-1x_U007emarketing-0-7-0-0-sRGB-85-220.png/350x350.png'},
    {key: '115', name: getPanName('115'), validator: is115Link, pic: 'https://img.pcsoft.com.cn/soft/202104/093230-608b5e2ed5912.jpg'},
    {key: 'quark', name: getPanName('quark'), validator: isQuarkLink, pic: 'https://ts1.cn.mm.bing.net/th/id/R-C.a0d60e6a72806738e6f0b711a979bdf5?rik=lp5C9t5sYlkrLw&riu=http%3a%2f%2fpic.2265.com%2fupload%2f2020-10%2f202010151719492792.png&ehk=Pv6rq3JxJvKe2y1QsdzssyZ4Ez4cwiKWmIvK0aMgxi0%3d&risl=&pid=ImgRaw&r=0'},
    {key: 'uc', name: getPanName('uc'), validator: isUcLink, pic: 'https://ts1.cn.mm.bing.net/th/id/R-C.421c96e47df7c9719403654ee4f7c281?rik=yiiEoGCTgDDc3w&riu=http%3a%2f%2fpic.9663.com%2fupload%2f2023-5%2f20235111411256277.png&ehk=R81N%2flXMrl%2bxpRlST8DtHXDfab6rzaMb83gihuD71Fk%3d&risl=&pid=ImgRaw&r=0'},
    {key: 'ali', name: getPanName('ali'), validator: isAliLink, pic: 'https://inews.gtimg.com/newsapp_bt/0/13263837859/1000'},
    {key: '123', name: getPanName('123'), validator: is123Link, pic: 'https://statics.123957.com/static/favicon.ico'},
    {key: 'baidu', name: getPanName('baidu'), validator: isBaiduLink, pic: 'https://nd-static.bdstatic.com/m-static/disk-home/img/logo.png'},
]
