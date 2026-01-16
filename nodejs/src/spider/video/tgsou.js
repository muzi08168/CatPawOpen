import {init as _init, detail as _detail, proxy, play, getPanInfos} from '../../util/pan.js';
import {getUrlCache, getChannelUsernameCache, getCountCache, getPicCache} from "../../website/tgsou.js";
import axios from "axios";
import {is115Link, is123Link, isAliLink, isQuarkLink, isTyLink, isUcLink, isYdLink} from "../../util/linkDetect.js";

async function init(inReq, _outResp) {
  await _init(inReq, _outResp);
  return {};
}

async function home(_inReq, _outResp) {
  return {
    class: [],
  };
}



async function category(inReq, _outResp) {
 return {
   page: 1,
   pagecount: 1,
   list: [],
 }
}

async function detail(inReq, _outResp) {
  const ids = !Array.isArray(inReq.body.id) ? [inReq.body.id] : inReq.body.id;
  const videos = [];
  for (const id of ids) {
    const vodFromUrl = await _detail(id, inReq);
    const vod = {}
    if (vodFromUrl){
      vod.vod_play_from = vodFromUrl.froms;
      vod.vod_play_url = vodFromUrl.urls;
    }
    videos.push(vod);
  }
  return {
    list: videos,
  };
}

async function search(inReq, _outResp) {
  const url = await getUrlCache(inReq.server)
  const count = await getCountCache(inReq.server)
  const channelUsername = await getChannelUsernameCache(inReq.server)
  const pic = await getPicCache(inReq.server)
  const wd = inReq.body.wd;
  const res = await axios.get(`${url}?pic=${pic}&count=${count}&channelUsername=${encodeURIComponent(channelUsername)}&keyword=${encodeURIComponent(wd)}`);
  const rs = []
  for (let item of res.data.results) {
    let [group, vodListStr] = item.split("$$$");
    if (!vodListStr) continue;
    const vodList = vodListStr.split("##");
    for (let p = 0; p < vodList.length && p <= count; p++) {
      const [linkInfo, title] = vodList[p].split("$$")
      const [link, imgId] = linkInfo.split('@')
      if (!rs.some(item => item.vod_id === link)) {
        const panInfo = getPanInfos().find(pan => pan.validator(link));
        if (panInfo) {
          rs.push({
            vod_id: link?.replace(/\s+/g, ''),
            vod_name: title?.replace(/\s+/g, '') || wd,
            vod_pic: imgId ? `${url}/down?id=${imgId}&channelUsername=${group}` : panInfo.pic,
            vod_remarks: `${panInfo.name}:${group}`,
          })
        }
      }
    }
  }
  return {
    page: 1,
    pagecount: 1,
    list: rs,
  };
}

export default {
  meta: {
    key: 'tgsou',
    name: 'tg搜(仅搜索)',
    type: 3,
  },
  api: async (fastify) => {
    fastify.post('/init', init);
    fastify.post('/home', home);
    fastify.post('/category', category);
    fastify.post('/detail', detail);
    fastify.post('/play', play);
    fastify.post('/search', search);
    fastify.get('/proxy/:site/:what/:flag/:shareId/:fileId/:end', proxy);
  },
};
