export const getCache = async (server) => {
  const obj = await server.db.getObjectDefault(`/danmu`)
  // 优先级：缓存->配置文件
  return obj || server.config.danmu
}

export const setCache = async (server, value) => {
  await server.db.push(`/danmu`, value);
}

export const removeCache = async (server) => {
  await server.db.delete(`/danmu`);
}

export default async function danmu(fastify) {
  fastify.get('/setting', async (req, res) => {
    res.send({
      code: 0,
      data: await getCache(req.server)
    })
  })

  fastify.put('/setting', async (req, res) => {
    await setCache(req.server, req.body)
    res.send({
      code: 0,
    })
  })

  fastify.delete('/setting', async (req, res) => {
    await removeCache(req.server)
    res.send({
      code: 0,
    })
  })

  fastify.post('/push', async (req, res) => {
    messageToDart({
      action: 'danmuPush',
      opt: {
        url: req.body.url
      }
    })
    res.send({
      code: 0,
    })
  })

  fastify.get('/fe', (req, res) => {
    res.type('text/html').send(`
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>弹幕搜索</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <link rel="stylesheet" href="https://lib.baomitu.com/antd/5.25.0/reset.min.css">      </head>
      <body>
        <div id="app"></div>
        <script crossorigin src="https://lib.baomitu.com/react/18.2.0/umd/react.production.min.js"></script>
        <script crossorigin src="https://lib.baomitu.com/react-dom/18.2.0/umd/react-dom.production.min.js"></script>
        <script crossorigin src="https://lib.baomitu.com/axios/0.26.0/axios.min.js"></script>
        <script crossorigin src="https://lib.baomitu.com/dayjs/1.10.8/dayjs.min.js"></script>
        <script crossorigin src="https://lib.baomitu.com/antd/5.25.0/antd.min.js"></script>
        <script>${globalThis.danmuBundle}</script>
      </body>
    </html>
    `)
  })
}
