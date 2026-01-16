export const ALL_PANS = [
  {
    "key": "quark",
    "name": "夸父",
    "enable": true
  },
  {
    "key": "tianyi",
    "name": "天意",
    "enable": true
  },
  {
    "key": "yidong",
    "name": "逸动",
    "enable": true
  },
  {
    "key": "123",
    "name": "Pan123",
    "enable": true
  },
  {
    "key": "115",
    "name": "Pan115",
    "enable": true
  },
  {
    "key": "uc",
    "name": "优夕",
    "enable": true
  },
  {
    "key": "baidu",
    "name": "百度",
    "enable": true
  },
  {
    "key": "ali",
    "name": "阿狸",
    "enable": true
  },
]

export const getCache = async (server) => {
  const obj = await server.db.getObjectDefault(`/pans`, {})
  const cacheList = obj?.list || server.config.pans.list
  return cacheList
    // 过滤掉已失效的
    .filter((item) => ALL_PANS.some(pan => pan.key === item.key))
    // 添加新增的
    .concat(ALL_PANS.filter(pan => !cacheList.some(item => item.key === pan.key))
  )
}

export const setCache = async (server, value) => {
  await server.db.push(`/pans/list`, value);
  globalThis.Pans = value
}

export const removeCache = async (server) => {
  await server.db.delete(`/pans/list`);
  globalThis.Pans = ALL_PANS
}

export default async function pans(fastify) {
  fastify.get('/list', async (req, res) => {
    res.send({
      code: 0,
      data: await getCache(req.server)
    })
  })

  fastify.put('/list', async (req, res) => {
    await setCache(req.server, req.body.list)
    res.send({
      code: 0,
    })
  })

  fastify.delete('/list', async (req, res) => {
    await removeCache(req.server)
    res.send({
      code: 0,
    })
  })
}
