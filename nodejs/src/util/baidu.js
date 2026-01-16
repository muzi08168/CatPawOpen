/**
 * 百度网盘解析模块
 * 提供百度网盘分享链接的解析和文件获取功能
 * 支持获取分享文件列表、生成播放链接等操作
 */
import axios from "axios";
import qs from "qs";
import CryptoJS from "crypto-js";
import {videosHandle} from "./utils.js";

const getPanName = async (key) => {
  return globalThis.getPanName ? await globalThis.getPanName(key) : key;
};

let db;
let localData = {};
const ENV = {
  set(key, value) {
    localData[key] = value;
    db.push(`/baidu/${key}`, value);
  },
  get(key) {
    return localData[key];
  }
};

export const initBaidu = async (req) => {
  db = req.server.db;
  localData = await db.getObjectDefault(`/baidu`, {});
};

/**
 * 解析分享链接，提取shareId和密码
 * @param {string} url - 百度网盘分享链接
 * @returns {Object|null} 分享数据对象 {shareId, sharePwd}
 */
export function getShareData(url) {
  const regex = /https:\/\/pan\.baidu\.com\/s\/(.*)\?.*?pwd=([^&]+)/;
  const matches = regex.exec(url);
  if (matches && matches[1]) {
    return {
      shareId: matches[1],
      sharePwd: matches[2] || '',
    };
  }
  return null;
}

/**
 * 百度网盘驱动类
 */
class BaiduDrive {
  constructor() {
    // 百度网盘分享链接正则表达式
    this.regex = /https:\/\/pan\.baidu\.com\/s\/(.*)\?.*?pwd=([^&]+)/;
    // 支持的视频质量类型
    this.type = ["M3U8_AUTO_4K", "M3U8_AUTO_2K", "M3U8_AUTO_1080", "M3U8_AUTO_720", "M3U8_AUTO_480"];
    // 请求头配置
    this.headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
      "Connection": "keep-alive",
      "Accept": "application/json, text/plain, */*",
      "Accept-Encoding": "gzip, deflate, br",
      "Accept-Language": "zh,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6"
    };
    // 百度网盘API基础地址
    this.api = 'https://pan.baidu.com';
    // 分享链接
    this.link = '';
    // 提取码
    this.pwd = '';
    // 短链接标识
    this.surl = '';
    // 短链接（去掉首字符）
    this.shorturl = '';
    // 分享ID
    this.shareid = '';
    // 应用ID
    this.app_id = 250528;
    // 视图模式
    this.view_mode = 1;
    // 渠道标识
    this.channel = 'chunlei';
  }

  /**
   * 获取百度网盘Cookie
   * @returns {string} 百度网盘Cookie
   */
  get cookie() {
    return ENV.get('baidu_cookie') || '';
  }

  /**
   * 解析分享链接，提取surl和密码
   * @param {string} url - 百度网盘分享链接
   */
  async getSurl(url) {
    this.link = url;
    const matches = this.regex.exec(url);
    if (matches && matches[1]) {
      this.surl = matches[1];
      // 去掉首字符的短链接
      this.shorturl = this.surl.split('').slice(1).join('');
      this.pwd = matches[2] || '';
    }
  }

  /**
   * 获取签名信息
   * @returns {Promise<string>} 签名字符串
   */
  async getSign() {
    // 确保headers中包含Cookie
    const headers = {
      ...this.headers,
      'cookie': this.cookie
    };
    let response = await axios.get(`${this.api}/share/tplconfig?surl=${this.surl}&fields=Espace_info,card_info,sign,timestamp&view_mode=${this.view_mode}&channel=${this.channel}&web=1&app_id=${this.app_id}`, {
      headers
    });

    if (!response.data || !response.data.data || !response.data.data.sign) {
      throw new Error(`获取sign失败: ${JSON.stringify(response.data)}`);
    }

    return response.data.data.sign;
  }

  /**
   * 获取随机密钥(randsk)并更新Cookie
   * @returns {Promise<string>} 随机密钥
   */
  async getRandsk() {
    // 构造验证请求数据
    let data = qs.stringify({
      'pwd': this.pwd,
      'vcode': '',
      'vcode_str': ''
    });
    // 发送验证请求获取randsk
    let randsk = (await axios.post(`${this.api}/share/verify?surl=${this.shorturl}&pwd=${this.pwd}`, data, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'Referer': this.link,
      }
    })).data.randsk;
    let BDCLND = "BDCLND=" + randsk;
    // 更新Cookie中的BDCLND值
    if (!this.cookie.includes('BDCLND')) {
      let cookie = this.cookie + BDCLND;
      ENV.set('baidu_cookie', cookie);
      return randsk;
    } else {
      let cookie = this.cookie.split(';').map(it => {
        if (/BDCLND/.test(it)) {
          it = BDCLND;
        }
        return it;
      }).join(';');
      if (cookie !== this.cookie) {
        ENV.set('baidu_cookie', cookie);
      }
      return randsk;
    }
  }

  /**
   * 获取分享文件列表
   * @returns {Promise<Object>} 文件列表对象
   */
  async getShareList() {
    await this.getRandsk();
    this.headers['cookie'] = this.cookie;
    // 获取分享根目录文件列表
    let data = (await axios.get(`${this.api}/share/list?web=5&app_id=${this.app_id}&desc=1&showempty=0&page=1&num=20&order=time&shorturl=${this.shorturl}&root=1&view_mode=${this.view_mode}&channel=${this.channel}&web=1&clienttype=0`, {
      headers: this.headers
    })).data;
    if (data.errno === 0 && data.list.length > 0) {
      let file = {};
      let dirs = []; // 目录列表
      let videos = []; // 视频文件列表
      this.uk = data.uk;
      this.shareid = data.share_id;

      // 缓存分享信息供play函数使用（使用surl作为key）
      ENV.set(`baidu_share_${this.surl}`, {
        surl: this.surl,
        pwd: this.pwd,
        link: this.link,
        uk: this.uk,
        shareid: this.shareid
      });
      // 遍历文件列表，分类处理
      data.list.map(item => {
        // 目录类型 (category: 6)
        if (item.category === '6' || item.category === 6) {
          dirs.push(item.path);
        }
        // 视频类型 (category: 1)
        if (item.category === '1' || item.category === 1) {
          // 确保所有情况下都提取文件名
          const fileName = item.server_filename || item.path.split('/').pop();
          videos.push({
            vod_name: fileName, // 只使用文件名
            // 文件ID格式: path*uk*shareid*fsid*surl
            vod_id: [item.path.replaceAll('#', '\0'), this.uk, this.shareid, item.fs_id || item.fsid, this.surl].join('*'),
            vod_size: item.size
          });
        }
      });
      // 初始化文件对象
      if (!(data.title in file) && data.title !== undefined) {
        file[data.title] = [];
      }
      if (videos.length >= 0 && data.title !== undefined) {
        file[data.title] = [...videos];
      }
      // 递归获取子目录中的文件
      let result = await Promise.all(dirs.map(async (path) => this.getSharepath(path)));
      result = result.filter(item => item !== undefined && item !== null).flat();
      if (result.length >= 0) {
        // 确保递归获取的文件也正确处理文件名
        const processedResult = result.map(item => {
          if (item.vod_name && item.vod_name.includes('/')) {
            item.vod_name = item.vod_name.split('/').pop();
          }
          return item;
        });
        file[data.title].push(...processedResult);
      }
      return file;
    }
  }

  /**
   * 获取指定路径下的文件列表（递归）
   * @param {string} path - 目录路径
   * @returns {Promise<Array>} 文件列表数组
   */
  async getSharepath(path) {
    await this.getRandsk();
    this.headers['cookie'] = this.cookie;
    // 获取指定目录下的文件列表
    let data = (await axios.get(`${this.api}/share/list?is_from_web=true&uk=${this.uk}&shareid=${this.shareid}&order=name&desc=0&showempty=0&view_mode=${this.view_mode}&web=1&page=1&num=100&dir=${encodeURIComponent(path)}&channel=${this.channel}&web=1&app_id=${this.app_id}`, {
      headers: this.headers
    })).data;
    if (data.errno === 0 && data.list.length > 0) {
      let dirs = []; // 子目录列表
      let videos = []; // 视频文件列表
      // 遍历当前目录文件
      data.list.map(item => {
        // 目录类型
        if (item.category === '6' || item.category === 6) {
          dirs.push(item.path);
        }
        // 视频类型
        if (item.category === '1' || item.category === 1) {
          // 确保所有情况下都提取文件名
          const fileName = item.server_filename || item.path.split('/').pop();
          videos.push({
            vod_name: fileName, // 只使用文件名
            // 文件ID格式: path*uk*shareid*fsid*surl
            vod_id: [item.path.replaceAll('#', '\0'), this.uk, this.shareid, item.fs_id || item.fsid, this.surl].join('*'),
            vod_size: item.size
          });
        }
      });
      // 递归处理子目录
      let result = await Promise.all(dirs.map(async (path) => this.getSharepath(path)));
      result = result.filter(item => item !== undefined && item !== null);

      // 确保递归获取的文件也正确处理文件名
      const processedResult = result.map(item => {
        if (item.vod_name && item.vod_name.includes('/')) {
          item.vod_name = item.vod_name.split('/').pop();
        }
        return item;
      });

      return [...videos, ...processedResult.flat()];
    }
  }

  /**
   * SHA1哈希计算
   * @param {string} message - 待哈希的消息
   * @returns {string} SHA1哈希值
   */
  sha1(message) {
    return CryptoJS.SHA1(message).toString(CryptoJS.enc.Hex);
  }

  /**
   * 获取用户UID
   * @returns {Promise<string>} 用户UID
   */
  async getUid() {
    this.headers['cookie'] = this.cookie;
    let data = (await axios.get('https://mbd.baidu.com/userx/v1/info/get?appname=baiduboxapp&fields=%20%20%20%20%20%20%20%20%5B%22bg_image%22,%22member%22,%22uid%22,%22avatar%22,%20%22avatar_member%22%5D&client&clientfrom&lang=zh-cn&tpl&ttt', {
      headers: this.headers
    })).data;
    if (data && data.data && data.data.fields && data.data.fields.uid) {
      return data.data.fields.uid;
    }
    throw new Error('获取用户UID失败: ' + JSON.stringify(data));
  }

  /**
   * 获取文件的播放链接（Web版）
   * @param {string} path - 文件路径
   * @param {string} uk - 用户标识
   * @param {string} shareid - 分享ID
   * @param {string} fsid - 文件ID
   * @returns {Promise<Array>} 不同清晰度的播放链接数组
   */
  async getShareUrl(path, uk, shareid, fsid) {
    path = path.replaceAll('\0', '#'); // 把真实路径还原
    let sign = await this.getSign();
    let urls = [];
    let t = Math.floor(new Date() / 1000); // 当前时间戳
    // 生成不同清晰度的播放链接
    this.type.map(it => {
      urls.push({
        name: it.replace('M3U8_AUTO_', ''),
        url: `${this.api}/share/streaming?channel=${this.channel}&uk=${uk}&fid=${fsid}&sign=${sign}&timestamp=${t}&shareid=${shareid}&type=${it}&vip=0&jsToken&isplayer=1&check_blue=1&adToken`
      });
    });
    return urls;
  }

  /**
   * 获取文件的直链地址（App版）
   * @param {string} path - 文件路径
   * @param {string} uk - 用户标识
   * @param {string} shareid - 分享ID
   * @param {string} fsid - 文件ID
   * @returns {Promise<string>} 直链地址
   */
  async getAppShareUrl(path, uk, shareid, fsid) {
    path = path.replaceAll('\0', '#'); // 把真实路径还原
    let BDCLND = await this.getRandsk();
    let uid = await this.getUid();
    // 设置移动端请求头
    let header = Object.assign({}, this.headers, {
      "User-Agent": 'netdisk;P2SP;2.2.91.136;android-android;',
    });
    let devuid = "73CED981D0F186D12BC18CAE1684FFD5|VSRCQTF6W"; // 设备标识
    let time = String(new Date().getTime()); // 时间戳
    // 生成签名
    let rand = this.sha1(this.sha1(this.cookie.match(/BDUSS=(.+?);/)[1]) + uid + "ebrcUYiuxaZv2XGu7KIYKxUrqfnOfpDF" + time + devuid + "11.30.2ae5821440fab5e1a61a025f014bd8972");
    // 构造请求URL
    let url = this.api + "/share/list?shareid=" + shareid + "&uk=" + uk + "&fid=" + fsid + "&sekey=" + BDCLND + "&origin=dlna&devuid=" + devuid + "&clienttype=1&channel=android_12_zhao_bd-netdisk_1024266h&version=11.30.2&time=" + time + "&rand=" + rand;
    let data = (await axios.get(url, {
      headers: header
    })).data;
    if (data.errno === 0 && data.list.length > 0) {
      return data.list[0].dlink; // 返回直链地址
    }
  }
}

const baiduDrive = new BaiduDrive();

/**
 * 获取文件详情（统一接口）
 * @param {string} shareUrl - 分享链接
 * @returns {Promise<Object>} 文件详情
 */
export async function detail(shareUrl, req) {
  await initBaidu(req)
  await baiduDrive.getSurl(shareUrl);
  const shareData = await baiduDrive.getShareList();
  if (!shareData) return null;

  const panName = await getPanName('baidu');
  const froms = [];
  const urls = [];

  Object.keys(shareData).forEach(it => {
    const data = videosHandle(panName + '-' + it, shareData[it]);
    if (data && data.from && data.url) {
      froms.push(data.from);
      urls.push(data.url);
    }
  });

  if (froms.length === 0) return null;

  return {
    from: froms.join('$$$'),
    url: urls.join('$$$')
  };
}

// 缓存播放URL
const baiduPlayUrlCache = {};

/**
 * 获取播放链接（统一接口）
 * @param {Object} inReq - 请求对象
 * @param {Object} _outResp - 响应对象
 * @returns {Promise<Object>} 播放信息
 */
export async function play(inReq, _outResp) {
  const ids = inReq.body.id.split('*');
  await initBaidu(inReq);

  // 从文件ID提取surl（文件ID格式: path*uk*shareid*fsid*surl）
  const surl = ids[4];

  // 从ENV读取缓存的分享信息
  const shareInfo = ENV.get(`baidu_share_${surl}`);

  if (shareInfo) {
    // 恢复分享链接信息
    baiduDrive.surl = shareInfo.surl;
    baiduDrive.pwd = shareInfo.pwd;
    baiduDrive.link = shareInfo.link;
    baiduDrive.uk = shareInfo.uk;
    baiduDrive.shareid = shareInfo.shareid;
    baiduDrive.shorturl = baiduDrive.surl ? baiduDrive.surl.split('').slice(1).join('') : '';

    // 确保已经验证过分享链接（获取randsk）
    if (baiduDrive.pwd) {
      await baiduDrive.getRandsk();
      // cookie是getter，会自动从ENV读取最新值（包括更新后的BDCLND）
      baiduDrive.headers['cookie'] = baiduDrive.cookie;
    }
  }

  // 构建代理URL（符合项目标准格式）
  const proxyUrl = inReq.server.address().url + inReq.server.prefix + '/proxy/baidu';
  // fileId格式: path*uk*shareid*fsid
  const fileId = `${ids[0]}*${ids[1]}*${ids[2]}*${ids[3]}`;
  const encodedFileId = encodeURIComponent(fileId);
  // shareId使用surl
  const shareId = surl;

  try {
    // 优先尝试App版API（直链，更稳定）
    const directUrl = await baiduDrive.getAppShareUrl(ids[0], ids[1], ids[2], ids[3]);
    if (directUrl) {
      // 缓存直链URL（使用surl作为key）
      baiduPlayUrlCache[surl] = {
        type: 'app',
        url: directUrl,
        fileId: fileId,
        headers: {
          "User-Agent": 'netdisk;P2SP;2.2.91.136;android-android;',
          "Referer": 'https://pan.baidu.com/'
        }
      };

      return {
        parse: 0,
        url: ['原画', `${proxyUrl}/src/redirect/${shareId}/${encodedFileId}/.bin`],
        header: {}
      };
    }
  } catch (error) {
    console.log('App版API失败，降级到Web版API:', error.message);
  }

  // 降级到Web版API（多清晰度，不需要UID）
  const urls = await baiduDrive.getShareUrl(ids[0], ids[1], ids[2], ids[3]);

  if (urls && urls.length > 0) {
    // 缓存Web版URL（使用surl作为key）
    baiduPlayUrlCache[surl] = {
      type: 'web',
      urls: urls,
      fileId: fileId,
      headers: {
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        "Referer": 'https://pan.baidu.com/'
      }
    };

    // 返回代理URL（符合标准格式）
    const urlList = urls.map(item => [item.name, `${proxyUrl}/src/${item.name}/${shareId}/${encodedFileId}/.bin`]);
    return {
      parse: 0,
      url: urlList,
      header: {}
    };
  }

  throw new Error('获取播放链接失败');
}

/**
 * 代理请求处理（符合项目标准格式）
 * @param {Object} inReq - 请求对象
 * @param {Object} outResp - 响应对象
 */
export async function proxy(inReq, outResp) {
  const site = inReq.params.site;
  const what = inReq.params.what;
  const shareId = inReq.params.shareId;  // surl
  const fileId = inReq.params.fileId;    // path*uk*shareid*fsid (已编码)
  const flag = inReq.params.flag;        // 清晰度或操作类型

  if (site !== 'baidu') {
    return outResp.status(400).send({ error: 'Invalid site' });
  }

  // 从缓存获取播放URL（使用shareId/surl作为key）
  const cached = baiduPlayUrlCache[shareId];
  if (!cached) {
    return outResp.status(404).send({ error: 'Play URL not found in cache' });
  }

  let targetUrl;
  let headers = cached.headers;

  if (cached.type === 'app') {
    // App版直链
    targetUrl = cached.url;
  } else if (cached.type === 'web') {
    // Web版多清晰度
    const urlInfo = cached.urls.find(u => u.name === flag);
    if (!urlInfo) {
      return outResp.status(404).send({ error: 'Quality not found' });
    }
    targetUrl = urlInfo.url;
  } else {
    return outResp.status(500).send({ error: 'Invalid cache type' });
  }

  // 重定向到实际URL
  if (flag === 'redirect') {
    return outResp.redirect(targetUrl);
  }

  // 代理流式传输
  return outResp.redirect(targetUrl);
}

