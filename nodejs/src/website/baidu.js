/**
 * 百度网盘扫码登录模块
 * 提供二维码生成、扫码状态检测、Cookie获取等功能
 */
import axios from "axios";
import qrcode from "qrcode";
import CryptoJS from "crypto-js";
import {getCookieArray} from "../util/misc.js";

const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36";

// 存储二维码数据
let qrData = {};

/**
 * 获取缓存的Cookie
 * @param {Object} server - Fastify服务器实例
 * @returns {Promise<string>} Cookie字符串
 */
export const getCache = async (server) => {
  const key = 'baidu_cookie';
  const obj = await server.db.getObjectDefault(`/baidu`, {});
  return obj[key] ?? '';
};

/**
 * 设置缓存的Cookie
 * @param {Object} server - Fastify服务器实例
 * @param {string} value - Cookie值
 */
export const setCache = async (server, value) => {
  const key = 'baidu_cookie';
  await server.db.push(`/baidu/${key}`, value);
};

/**
 * 生成UUID
 * @returns {string} UUID字符串
 */
function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

/**
 * 百度网盘扫码登录路由
 */
export default async function baidu(fastify) {
  /**
   * 获取二维码
   * GET /website/baidu/qrcode
   */
  fastify.get('/qrcode', async (req, res) => {
    try {
      const requestId = generateUUID();
      const t3 = new Date().getTime().toString();
      const t1 = Math.floor(new Date().getTime() / 1000).toString();
      const callback = `tangram_guid_${t3}`;

      // 请求百度二维码API
      const response = await axios.get('https://passport.baidu.com/v2/api/getqrcode', {
        params: {
          lp: 'pc',
          qrloginfrom: 'pc',
          gid: requestId,
          callback: callback,
          apiver: 'v3',
          tt: t1,
          tpl: 'netdisk',
          _: t3
        },
        headers: {
          'User-Agent': UA,
          'Referer': 'https://pan.baidu.com',
          'sec-ch-ua-platform': '"Windows"',
          'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
          'DNT': '1',
          'sec-ch-ua-mobile': '?0',
          'Sec-Fetch-Site': 'same-site',
          'Sec-Fetch-Mode': 'no-cors',
          'Sec-Fetch-Dest': 'script',
          'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
        }
      });

      // 解析JSONP响应
      let responseText = response.data;
      if (typeof responseText === 'string') {
        // 移除JSONP回调函数包装
        responseText = responseText.replace(/^[^(]*\(/, '').replace(/\);?$/, '');
        const data = JSON.parse(responseText);

        if (data.errno === 0) {
          // 保存二维码数据
          qrData = {
            sign: data.sign,
            imgurl: data.imgurl,
            channel_id: data.channel_id,
            t1: t1,
            t3: t3,
            requestId: requestId,
            callback: callback
          };

          // 获取二维码图片
          const qrcodeImageUrl = 'https://' + data.imgurl;
          const qrcodeImageResponse = await axios.get(qrcodeImageUrl, {
            responseType: 'arraybuffer',
            headers: {
              'User-Agent': UA,
            }
          });

          res.send(qrcodeImageResponse.data);
        } else {
          throw new Error(`获取二维码失败: ${data.errmsg || '未知错误'}`);
        }
      } else {
        throw new Error('响应格式错误');
      }
    } catch (error) {
      console.error('百度二维码获取失败:', error);
      res.status(500).send({
        code: -1,
        message: error?.message || '获取二维码失败'
      });
    }
  });

  /**
   * 检测扫码状态并获取Cookie
   * POST /website/baidu/cookie
   */
  fastify.post('/cookie', async (req, res) => {
    try {
      if (!qrData.sign) {
        throw new Error('请先获取二维码');
      }

      // 轮询检测扫码状态（最多30次，每次间隔1秒）
      for (let i = 0; i < 30; i++) {
        const t = new Date().getTime().toString();
        const callback = `tangram_guid_${t}`;

        // 检测扫码状态
        const statusResponse = await axios.get('https://passport.baidu.com/channel/unicast', {
          params: {
            channel_id: qrData.sign,
            tpl: 'netdisk',
            apiver: 'v3',
            tt: Math.floor(new Date().getTime() / 1000).toString(),
            callback: callback,
            _: t
          },
          headers: {
            'User-Agent': UA,
            'Referer': 'https://pan.baidu.com',
          }
        });

        // 解析JSONP响应
        let statusText = statusResponse.data;
        if (typeof statusText === 'string') {
          statusText = statusText.replace(/^[^(]*\(/, '').replace(/\);?\s*$/, '').trim();
          const statusData = JSON.parse(statusText);

          // 状态码说明:
          // channel_v.status: 1 - 等待授权, 0 - 已授权
          if (statusData.errno === 0 && statusData.channel_v) {
            const channelV = JSON.parse(statusData.channel_v);

            if (channelV.status === 1) {
              // 等待授权，继续轮询
              await new Promise(resolve => setTimeout(resolve, 1000));
              continue;
            }

            if (channelV.status === 0 && channelV.v) {
              // 扫码成功，获取BDUSS
              const bduss = channelV.v;

              // 调用qrbdusslogin获取完整Cookie
              const t3 = new Date().getTime().toString();
              const t1 = Math.floor(new Date().getTime() / 1000).toString();

              const loginResponse = await axios.get('https://passport.baidu.com/v3/login/main/qrbdusslogin', {
                params: {
                  'v': t3,
                  'bduss': bduss,
                  'u': 'https://pan.baidu.com/disk/main#/index?category=all',
                  'loginVersion': 'v5',
                  'qrcode': '1',
                  'tpl': 'netdisk',
                  'maskId': '',
                  'fileId': '',
                  'apiver': 'v3',
                  'tt': t3,
                  'traceid': '',
                  'time': t1,
                  'alg': 'v3',
                  'elapsed': '1'
                },
                headers: {
                  'User-Agent': UA,
                  'Referer': 'https://pan.baidu.com/',
                  'Accept-Language': 'zh-CN,zh;q=0.9'
                },
                timeout: 30000
              });

              // 解析JSONP响应
              let loginText = loginResponse.data;
              if (typeof loginText === 'string') {
                loginText = loginText.replace(/^[^(]*\(/, '').replace(/\);?\s*$/, '').trim();

                // 提取BDUSS, STOKEN, PTOKEN, UBI
                const bdussMatch = loginText.match(/"bduss":\s*"([^"]+)"/);
                const stokenMatch = loginText.match(/"stoken":\s*"([^"]+)"/);
                const ptokenMatch = loginText.match(/"ptoken":\s*"([^"]+)"/);
                const ubiMatch = loginText.match(/"ubi":\s*"([^"]+)"/);

                if (bdussMatch && stokenMatch && ptokenMatch && ubiMatch) {
                  const bduss = bdussMatch[1];
                  const stoken = stokenMatch[1];
                  const ptoken = ptokenMatch[1];
                  const ubi = encodeURIComponent(ubiMatch[1]);

                  // 构造完整Cookie用于认证
                  const cookies = {
                    'newlogin': '1',
                    'UBI': ubi,
                    'STOKEN': stoken,
                    'BDUSS': bduss,
                    'PTOKEN': ptoken,
                    'BDUSS_BFESS': bduss,
                    'STOKEN_BFESS': stoken,
                    'PTOKEN_BFESS': ptoken,
                    'UBI_BFESS': ubi,
                  };

                  // 构造Cookie字符串（不应URL编码）
                  const buildCookie = (params) => {
                    return Object.entries(params)
                      .map(([k, v]) => `${k}=${v}`)
                      .join('; ');
                  };

                  const cookieString = buildCookie(cookies);

                  // 调用auth API激活Cookie
                  const authResponse = await axios.get(
                    'https://passport.baidu.com/v3/login/api/auth/?return_type=5&tpl=netdisk&u=https://pan.baidu.com/disk/home',
                    {
                      headers: {
                        'User-Agent': UA,
                        'Referer': 'https://pan.baidu.com/',
                        'Cookie': cookieString
                      },
                      maxRedirects: 0,
                      validateStatus: (status) => status === 302 || status === 200
                    }
                  );

                  // 跟随重定向获取最终STOKEN
                  const location = authResponse.headers.location;
                  if (location) {
                    const finalResponse = await axios.get(location, {
                      headers: {
                        'User-Agent': UA,
                        'Referer': 'https://pan.baidu.com/',
                        'Cookie': cookieString
                      },
                      maxRedirects: 0,
                      validateStatus: (status) => status === 302 || status === 200
                    });

                    // 提取最终STOKEN
                    const setCookie = finalResponse.headers['set-cookie'];
                    let finalStoken = '';
                    if (setCookie) {
                      const stokenCookie = setCookie.find(c => c.toLowerCase().includes('stoken'));
                      if (stokenCookie) {
                        finalStoken = stokenCookie.split(';')[0];
                      }
                    }

                    // 构造最终Cookie
                    const finalCookie = finalStoken
                      ? `BDUSS=${bduss};${finalStoken};`
                      : `BDUSS=${bduss};STOKEN=${stoken};`;

                    // 保存Cookie到配置
                    await setCache(req.server, finalCookie);

                    res.send({
                      code: 0,
                      data: finalCookie,
                      message: '扫码成功'
                    });

                    // 清空二维码数据
                    qrData = {};
                    return;
                  } else {
                    // 没有重定向，直接使用原始Cookie
                    const cookie = `BDUSS=${bduss};STOKEN=${stoken};`;
                    await setCache(req.server, cookie);

                    res.send({
                      code: 0,
                      data: cookie,
                      message: '扫码成功'
                    });

                    qrData = {};
                    return;
                  }
                } else {
                  throw new Error('Cookie解析失败: 缺少必要字段');
                }
              }
            }
          } else if (statusData.errno === 1) {
            // 等待扫码，继续轮询
            await new Promise(resolve => setTimeout(resolve, 1000));
            continue;
          } else if (statusData.errno === 2) {
            // 二维码已失效
            throw new Error('二维码已失效，请重新获取');
          }
        }
      }

      // 超时
      throw new Error('扫码超时，请重试');

    } catch (error) {
      console.error('百度扫码登录失败:', error);
      res.send({
        code: -1,
        message: error?.message || '扫码登录失败'
      });
    }
  });

  /**
   * 获取当前Cookie
   * GET /website/baidu/cookie
   */
  fastify.get('/cookie', async (req, res) => {
    res.send({
      code: 0,
      data: await getCache(req.server)
    });
  });

  /**
   * 手动设置Cookie
   * PUT /website/baidu/cookie
   */
  fastify.put('/cookie', async (req, res) => {
    await setCache(req.server, req.body.cookie);
    res.send({
      code: 0,
      message: 'Cookie已保存'
    });
  });
}

