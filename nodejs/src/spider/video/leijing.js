import req from '../../util/req.js';
import {init, proxy, play, detail as _detail} from '../../util/pan.js';
import {jsoup} from "../../util/htmlParser.js";
import axios from "axios";
import {PC_UA} from "../../util/misc.js";
import {getCache} from "../../website/leijing.js";
import {isTyLink} from "../../util/linkDetect.js";
import {firstSuccessfulUrl} from "../../util/utils.js";

const headers = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
  'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
}

let getUrlPromise
const getUrl = (server) => {
  if (!getUrlPromise) {
    let timeStart = Date.now()
    getUrlPromise = new Promise(async (resolve) => {
      const urls = await getCache(server)
      const url = await firstSuccessfulUrl(urls, headers)
      console.log(`雷鲸域名`, url, `${Date.now() - timeStart}ms`)
      resolve(url)
    })
  }
  return getUrlPromise
}

async function getHtml(config) {
  try {
    return await axios.request(typeof config === "string" ? config : {
      url: config.url,
      method: config.method || 'GET',
      headers: config.headers || {
        ...headers,
        'User-Agent': PC_UA
      },
      data: config.data || '',
      responseType: config.responseType || '',//'arraybuffer'
    })
  } catch (e) {
    return e.response
  }
}

async function request(reqUrl) {
  const resp = await req.get(reqUrl, {
    headers,
  });
  return resp.data;
}

const pq = (html) => {
  const jsp = new jsoup();
  return jsp.pq(html);
}

async function home(_inReq, _outResp) {
  let classes = [
    {type_id: '42204681950354', type_name: '电影',},
    {type_id: '42204684250355', type_name: '剧集',},
    {type_id: '42212287587456', type_name: '影视原盘',},
    {type_id: '42204697150356', type_name: '记录',},
    {type_id: '42204792950357', type_name: '动画动漫',},
    {type_id: '42210356650363', type_name: " 综艺"}
  ];
  let filterObj = {};
  return ({
    class: classes,
    filters: filterObj,
  });
}

async function category(inReq, _outResp) {
  const url = await getUrl(inReq.server)
  const tid = inReq.body.id;
  const pg = inReq.body.page;
  let page = pg || 1;
  if (page == 0) page = 1;
  let html = await request(`${url}/?tagId=${tid}&page=${page}`)
  const $ = pq(html)
  let videos = []
  $('.topicList .topicItem').each((index, item) => {
    const a = $(item).find('h2 a:first')[0];
    videos.push({
      "vod_name": a.children[0].data,
      "vod_id": a.attribs.href,
      "vod_pic": inReq.server.address().url + inReq.server.prefix + '/icon'
    })
  })
  const pageInfo = $('.topicPage .pg')[0]
  const total = $(pageInfo).find('.count')[0].children[0].data.match(/\d+/)[0]
  const pageCount = $(pageInfo).find('.last')[0].attribs.title.match(/\d+/)[0]
  return ({
    page: parseInt(page),
    pagecount: pageCount ? parseInt(pageCount) : 1,
    limit: videos.length,
    total: total ? parseInt(total) : videos.length,
    list: videos,
  });
}

async function detail(inReq, _outResp) {
  const url = await getUrl(inReq.server)
  let html = await request(`${url}/${inReq.body.id}`)
  const $ = pq(html)
  let vod = {
    "vod_name": $('.title')[0].children[0].data.trim(),
    "vod_id": `${inReq.body.id}`,
    "vod_content": $('div.topicContent p:nth-child(1)').text().replace(/\s+/g, ''),
  }
  let content_html = $('.topicContent').html()
  let link = content_html.match(/<a\s+(?:[^>]*?\s+)?href=["'](https?:\/\/cloud\.189\.cn\/[^"']*)["'][^>]*>/gi);
  if (!link || link.length === 0) {
    // 如果 a 标签匹配不到，尝试匹配 span 标签中的文本内容
    link = content_html.match(/<span\s+style="color:\s*#0070C0;\s*">https?:\/\/cloud\.189\.cn\/[^<]*<\/span>/gi);
    if (link && link.length > 0) {
      // 提取 span 标签中的 URL
      link = link[0].match(/https?:\/\/cloud\.189\.cn\/[^<]*/)[0];
    } else {
      link = content_html.match(/https?:\/\/cloud\.189\.cn\/[^<]*/)[0]
    }
  } else {
    // 提取 a 标签中的 URL
    link = link[0].match(/https?:\/\/cloud\.189\.cn\/[^"']*/)[0];
  }
  if (isTyLink(link)) {
    const vodFromUrl = await _detail(link, inReq);
    if (vodFromUrl){
      vod.vod_play_from = vodFromUrl.froms;
      vod.vod_play_url = vodFromUrl.urls;
    }
  }
  return {
    list: [vod],
  };
}


async function search(inReq, _outResp) {
  const url = await getUrl(inReq.server)
  const pg = inReq.body.page || 1;
  const wd = inReq.body.wd;
  let html = (await getHtml(`${url}/search?keyword=${encodeURIComponent(decodeURIComponent(wd))}&page=${pg}`)).data
  const $ = pq(html)
  let videos = []
  $('.topicList .topicItem').each((index, item) => {
    const tag = $(item).find('.tag')[0];
    if(['电影', '剧集', '动漫', '影视原盘', '综艺'].includes($(tag).html())) {
      const a = $(item).find('.title a:first')[0];
      videos.push({
        "vod_name": $(a).html().replace(/<[^>]*>/g, '').replace(/\s+/g, ''),
        "vod_id": a.attribs.href,
        "vod_remarks": '',
        "vod_pic": `${url}/favicon.ico`
      })
    }
  })
  return {
    page: pg,
    pagecount: 1,
    list: videos,
  };
}

async function icon(inReq, reply) {
  try {
    const base64Image = "iVBORw0KGgoAAAANSUhEUgAAAEAAAABABAMAAABYR2ztAAAALVBMVEVHcEw9r+cipOQqp+VItOhCsedives7ruccouNVueovquW74/eV1PLW7vpzxu6kNLMaAAAACHRSTlMAK9qm1B0KZtVpWLIAAAJCSURBVEjHxZUxj9MwFMdzQzsD1cFMGegKJ3QnPgITEirz3cAxw0l8gJbBl4E5TqssDCShyh45Ye+16X5y3JmkqT8Dz0lo4zSpN/iryZD30/s/1/Z7mvbv1B0+rGh4WY9/eHLWr+js03s5fnXT15G9+yG7f/64Gu/cDFBNo/O3FaB3EAfiRcXgJWrQs51Jt4catUtxctEMjB+UwLtBMzB6VTp8Ri16fnnUYefxcdAGjL4UJaBWFUU8ageeHq8RoR+iys7rdmAs9qPzph34rgK+inWeCEB3C4mv+1cFcJaFbGDzV2bIgEcIYfBAzKOmi/TotwToDsZTijGGzD/pcu46ke/YFcCD7AsmPAx9G4T+hDJK7irAjJYAMzyKo3QqWAlYW9yKLZ5ExoRmbGWCm1m1mPkbwuMpmQPAQsYwCYhfLdKblxkywyFTP8J0tVhLy3SzIM8AC8yB2J7dSwCyVnhlLVNYcRhSdgjoUFUCjw0ZNr/Agm9kCy/M/0lq5BYbDIVKRaJvMc8o56EAFkFo4mhuyhlibNHEIgAsYEvMZJukcgZGQrBgdr4t2Iu32dqWLZIpZACLW2xlq8n9lkM9EgAWWFjMYJuC6G6bRmnNglCwMGBjOXaIFaaTtJZBSBTpQh3JjBquVMNtkp9Bbpen1Uls+cihIuI2nepjF6e4FxeKm3XsbuYdpKu63VpP1R+UHUbZo5RdrrVJ/e2T6k6r7NXaVWOK8X6kdFXzQj1xxMyS47WZpZ56mnY9PN2PzdPDufk/9QdaMC7B2rkgYwAAAABJRU5ErkJggg=="
    const imgBuffer = Buffer.from(base64Image, 'base64');

    reply.header('Content-Type', "image/png");
    reply.header('Content-Length', imgBuffer.length);

    return reply.send(imgBuffer);
  } catch (err) {
    reply.code(500).send({ error: 'Failed to load image' });
  }
}

export default {
  meta: {
    key: 'leijing',
    name: '雷鲸',
    type: 3,
  },
  api: async (fastify) => {
    fastify.post('/init', init);
    fastify.post('/home', home);
    fastify.post('/category', category);
    fastify.post('/detail', detail);
    fastify.post('/play', play);
    fastify.post('/search', search);
    fastify.get('/icon', icon);
    fastify.get('/proxy/:site/:what/:flag/:shareId/:fileId/:end', proxy);
  },
  check: getUrl
};
