import req from '../../util/req.js';
import {init, proxy, play, detail as _detail} from '../../util/pan.js';
import {jsoup} from "../../util/htmlParser.js";
import axios from "axios";
import {PC_UA} from "../../util/misc.js";
import {Yun} from "../../util/yun.js";

async function getHtml(config) {
  try {
    return await axios.request(typeof config === "string" ? config : {
      url: config.url,
      method: config.method || 'GET',
      headers: config.headers || {
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
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    },
  });
  return resp.data;
}

const url = 'https://www.91panta.cn'

const pq = (html) => {
  const jsp = new jsoup();
  return jsp.pq(html);
}

const classes = [
  {type_id: '39765285016165', type_name: '电影',},
  {type_id: '39765284616164', type_name: '电视剧',},
  {type_id: '39724839640293', type_name: '综艺',},
  {type_id: '39724838540291', type_name: " 动漫"},
  {type_id: '44732560408431', type_name: '短剧',},
  {type_id: '39956600861068', type_name: '纪录片',},
]

const tags = classes.map(item => item.type_name)

async function home(_inReq, _outResp) {
  let filterObj = {};
  return ({
    class: classes,
    filters: filterObj,
  });
}

async function category(inReq, _outResp) {
  const tid = inReq.body.id;
  const pg = inReq.body.page;
  let page = pg || 1;
  if (page == 0) page = 1;
  let html = await request(`${url}/?tagId=${tid}&page=${page}`)
  const $ = pq(html)
  let videos = []
  $('.topicList .topicItem').each((index, item) => {
    const a = $(item).find('h2 a:first')[0];
    const imgSrc = $(item).find('.tm-m-photos-thumb li:first')[0]?.attribs['data-src'];
    videos.push({
      "vod_name": a.children[0].data.split(' ')[0],
      "vod_id": a.attribs.href,
      "vod_pic": imgSrc ? `https://www.91panta.cn/${imgSrc}` : 'https://www.91panta.cn/favicon.ico',
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
  let html = await request(`${url}/${inReq.body.id}`)
  const $ = pq(html)
  let vod = {
    "vod_name": $('.title')[0].children[0].data.trim(),
    "vod_id": `/${inReq.body.id}`,
    "vod_content": $('div.topicContent p:nth-child(1)').text().replace(/\s+/g, ''),
  }
  let content_html = $('.topicContent').html()
  let link = content_html.match(/<a\s+(?:[^>]*?\s+)?href=["'](https:\/\/caiyun\.139\.com\/[^"']*)["'][^>]*>/gi);
  if (!link || link.length === 0) {
    // 如果 a 标签匹配不到，尝试匹配 span 标签中的文本内容
    link = content_html.match(/<span\s+style="color:\s*#0070C0;\s*">https:\/\/caiyun\.139\.com\/[^<]*<\/span>/gi);
    if (link && link.length > 0) {
      // 提取 span 标签中的 URL
      link = link[0].match(/https:\/\/caiyun\.139\.com\/[^<]*/)[0];
    } else {
      link = content_html.match(/https:\/\/caiyun\.139\.com\/[^<]*/)[0]
    }
  } else {
    // 提取 a 标签中的 URL
    link = link[0].match(/https:\/\/caiyun\.139\.com\/[^"']*/)[0];
  }
  const vodFromUrl = await _detail(link, inReq);
  if (vodFromUrl){
    vod.vod_play_from = vodFromUrl.froms;
    vod.vod_play_url = vodFromUrl.urls;
  }
  return {
    list: [vod],
  };
}


async function search(inReq, _outResp) {
  const pg = inReq.body.page || 1;
  const wd = inReq.body.wd;
  let html = (await getHtml(`${url}/search?keyword=${encodeURIComponent(decodeURIComponent(wd))}&page=${pg}`)).data
  const $ = pq(html)
  let videos = []
  $('.topicList .topicItem').each((index, item) => {
    const tag = $(item).find('.tag')[0];
    if(tags.includes($(tag).html())) {
      const a = $(item).find('.title a:first')[0];
      const imgSrc = $(item).find('.tm-m-photos-thumb li:first')[0]?.attribs['data-src'];
      videos.push({
        "vod_name": $(a).html().replace(/<[^>]*>/g, '').replace(/\s+/g, ''),
        "vod_id": a.attribs.href,
        "vod_remarks": '',
        "vod_pic": imgSrc ? `https://www.91panta.cn/${imgSrc}` : 'https://www.91panta.cn/favicon.ico',
      })
    }
  })
  return {
    page: pg,
    pagecount: 1,
    list: videos,
  };
}


export default {
  meta: {
    key: 'panta',
    name: '盘Ta',
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
