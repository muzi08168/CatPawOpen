export const getCache = async (server) => {
  const obj = await server.db.getObjectDefault(`/cms`, {})
  // 优先级：缓存->配置文件->兜底域名
  return obj?.list || server.config.cms?.list || []
}

export const setCache = async (server, value) => {
  await server.db.push(`/cms/list`, value);
}

export const removeCache = async (server) => {
  await server.db.delete(`/cms/list`);
}

export default async function cms(fastify) {
  fastify.get('/list', async (req, res) => {
    res.send({
      code: 0,
      data: await getCache(req.server)
    })
  })

  fastify.put('/list', async (req, res) => {
    await setCache(req.server, req.body)
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
