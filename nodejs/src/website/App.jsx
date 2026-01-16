import React, {useEffect, useMemo, useState} from 'react';
import { createRoot } from 'react-dom/client';
import {
  Button,
  Card,
  Form,
  Input,
  Tabs,
  message,
  Divider,
  Space,
  InputNumber,
  Row,
  Col,
  Switch,
  Alert,
  Table,
  Upload,
  Tag
} from 'antd';
import axios from 'axios'
import copy from 'copy-to-clipboard';
import { MinusCircleOutlined, PlusOutlined, UpOutlined, DownOutlined } from '@ant-design/icons';
import { DndContext, PointerSensor, useSensor, useSensors } from '@dnd-kit/core';
import { restrictToVerticalAxis } from '@dnd-kit/modifiers';
import {
  arrayMove,
  SortableContext,
  useSortable,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import './App.less'
import refreshImg from './assets/qrcode_refresh.jpg'

const { TabPane } = Tabs;

const http = axios.create({
  baseURL: '/website',
})

http.interceptors.response.use(function (response) {
  if (response.data.code !== 0) {
    message.error(response.data.message);
    throw new Error(response.data);
  } else {
    return response.data.data;
  }
}, function (error) {
  return Promise.reject(error);
});

message.config({
  top: '40%',
});

function QrcodeImage({src}) {
  const [inited, setInited] = React.useState(false);
  return (
    <img src={inited ? src : refreshImg} onClick={() => setInited(true)}/>
  )
}

function QrcodeCard({qrcodeUrl, cacheUrl}) {
  const [data, setData] = React.useState('');

  const generateData = async () => {
    const data = await http.post(cacheUrl)
    setData(data)
  }

  const storeData = async () => {
    await http.put(cacheUrl, {
      cookie: data,
    })
    message.success('入库成功')
  }

  const copyText = (text) => {
    copy(text)
    message.success('复制成功')
  }

  useEffect(() => {
    http.get(cacheUrl)
      .then(data => {
        setData(data);
      })
  }, [])

  return (
    <div className={'qrcodeCard'}>
      <QrcodeImage src={qrcodeUrl}/>
      <Divider>
        <Button onClick={generateData}>扫码后点我</Button>
      </Divider>
      <Input.TextArea value={data} onChange={e => setData(e.target.value)} rows={4}/>
      <div className={'btns'}>
        <Button onClick={() => copyText(data)} color="cyan" variant="solid"
                style={{marginRight: 24}} size={'large'}>复制</Button>
        <Button onClick={storeData} type={'primary'} size={'large'}>入库</Button>
      </div>
    </div>
  )
}

function AliQrcodeCard() {
  const [data, setData] = React.useState({
    token: '',
    token280: ''
  });
  const cacheUrl = '/ali/token'

  const generateData = async () => {
    const data = await http.post(cacheUrl)
    setData(data)
  }

  const storeData = async () => {
    await http.put(cacheUrl, {
      data,
    })
    message.success('入库成功')
  }

  useEffect(() => {
    http.get(cacheUrl)
      .then(data => {
        setData(data);
      })
  }, [])

  return (
    <div className={'qrcodeCard'}>
      <QrcodeImage src={'/website/ali/qrcode'}/>
      <Divider>
        <Button onClick={generateData}>扫码后点我</Button>
      </Divider>
      <Row>
        <Col span={11}>
          <p>Token</p>
          <Input.TextArea
            value={data.token}
            onChange={e => setData({token: e.target.value, token280: data.token280})}
            rows={4}
          />
        </Col>
        <Col span={11} offset={2}>
          <p>OpenToken</p>
          <Input.TextArea
            value={data.token280}
            onChange={e => setData({token: data.token, token280: e.target.value})}
            rows={4}
          />
        </Col>
      </Row>
      <div className={'btns'}>
        <Button onClick={storeData} disabled={!data} type={'primary'} size={'large'}>入库</Button>
      </div>
    </div>
  )
}

function SiteDomainSetting({api, name}) {
  const [form] = Form.useForm();
  const formItemLayout = {
    labelCol: { span: 6 },
    wrapperCol: { span: 18 },
  };
  const formItemLayoutWithOutLabel = {
    wrapperCol: { span: 18, offset: 6 },
  };

  const init = async () => {
    const data = await http.get(api)
    form.setFieldsValue({urls: data})
  }

  const submit = async () => {
    const data = await form.validateFields()
    try {
      await http.put(api, data.urls)
      message.success('入库成功')
    } catch (e) {
      console.error(e)
      message.error(`入库失败：${e?.message}`)
    }
  }

  const reset = async () => {
    try {
      await http.delete(api)
      init()
      message.success('重置成功')
    } catch (e) {
      console.error(e);
      message.error(`重置失败：${e?.message}`)
    }
  }

  useEffect(() => {
    init()
  }, [])

  return (
    <Form form={form} {...formItemLayout}>
      <Alert message="启动时将自动选择速度最快的域名" type="info" style={{ marginBottom: 12 }}/>
      <Form.List
        name="urls"
        rules={[
          {
            required: true,
            message: '请添加域名'
          },
        ]}
      >
        {(fields, { add, remove }, { errors }) => (
          <>
            {fields.map((field, index) => (
              <Form.Item
                label={index === 0 ? '域名列表' : ''}
                required={false}
                key={field.key}
                {...(index === 0 ? formItemLayout : formItemLayoutWithOutLabel)}
                style={{marginBottom: 12}}
              >
                <Form.Item
                  {...field}
                  validateTrigger={['onChange', 'onBlur']}
                  rules={[
                    {
                      required: true,
                      whitespace: true,
                      message: `请输入${name}域名`,
                    },
                  ]}
                  noStyle
                >
                  <Input placeholder={`请输入${name}域名`} style={{ width: '60%' }}/>
                </Form.Item>
                {fields.length > 1 ? (
                  <MinusCircleOutlined
                    className="dynamic-delete-button"
                    onClick={() => remove(field.name)}
                  />
                ) : null}
              </Form.Item>
            ))}
            <Form.Item label={''} {...formItemLayoutWithOutLabel}>
              <Button
                type="dashed"
                onClick={() => add()}
                style={{ width: '60%' }}
                icon={<PlusOutlined />}
              >
                添加域名
              </Button>
              <Form.ErrorList errors={errors} />
            </Form.Item>
          </>
        )}
      </Form.List>
      <Form.Item label={null}>
        <Button type="primary" onClick={submit}>
          保存
        </Button>
        <Button danger style={{marginLeft: 16}} onClick={reset}>重置</Button>
      </Form.Item>
    </Form>
  )
}

function AccountInfo({api}) {
  const [form] = Form.useForm();
  const submit = async () => {
    try {
      const data = await form.validateFields()
      await http.put(api, data)
      message.success('入库成功')
    } catch (e) {
      console.error(e)
      message.error(`入库失败：${e?.message}`)
    }
  }

  useEffect(() => {
    http.get(api)
      .then(data => {
        form.setFieldsValue(data)
      })
  }, [])

  return (
    <Form form={form}>
      <Form.Item label={"账号"} name="username">
        <Input/>
      </Form.Item>
      <Form.Item label={"密码"} name="password">
        <Input.Password/>
      </Form.Item>
      <Form.Item label={null}>
        <Button type="primary" onClick={submit}>
          保存
        </Button>
      </Form.Item>
    </Form>
  )
}

function TGSou() {
  const [form] = Form.useForm();
  const formItemLayout = {
    labelCol: { span: 6 },
    wrapperCol: { span: 18 },
  };
  const formItemLayoutWithOutLabel = {
    wrapperCol: { span: 18, offset: 6 },
  };

  const submit = async () => {
    const data = await form.validateFields()
    console.log('data', data)
    try {
      await http.put('/tgsou/config', data)
      message.success('入库成功')
    } catch (e) {
      console.error(e)
      message.error(`入库失败：${e?.message}`)
    }
  }

  useEffect(() => {
    http.get('/tgsou/config')
      .then(data => {
        form.setFieldsValue(data)
      })
  }, [])

  return (
    <Form form={form} {...formItemLayout}>
      <Form.Item label={"服务器地址"} name="url">
        <Input/>
      </Form.Item>
      <Form.List
        name="channelUsername"
        rules={[
          {
            required: true,
            message: '请添加频道'
          },
        ]}
      >
        {(fields, { add, remove }, { errors }) => (
          <>
            {fields.map((field, index) => (
              <Form.Item
                label={index === 0 ? '频道列表' : ''}
                required={false}
                key={field.key}
                {...(index === 0 ? formItemLayout : formItemLayoutWithOutLabel)}
                style={{marginBottom: 12}}
              >
                <Form.Item
                  {...field}
                  validateTrigger={['onChange', 'onBlur']}
                  rules={[
                    {
                      required: true,
                      whitespace: true,
                      message: "请输入频道名",
                    },
                  ]}
                  noStyle
                >
                  <Input placeholder="请输入频道名" style={{ width: '60%' }}/>
                </Form.Item>
                {fields.length > 1 ? (
                  <MinusCircleOutlined
                    className="dynamic-delete-button"
                    onClick={() => remove(field.name)}
                  />
                ) : null}
              </Form.Item>
            ))}
            <Form.Item label={''} {...formItemLayoutWithOutLabel}>
              <Button
                type="dashed"
                onClick={() => add()}
                style={{ width: '60%' }}
                icon={<PlusOutlined />}
              >
                添加频道
              </Button>
              <Form.ErrorList errors={errors} />
            </Form.Item>
          </>
        )}
      </Form.List>
      <Form.Item label={"单频道资源数量"} name="count">
        <InputNumber min={1}/>
      </Form.Item>
      <Form.Item label={"显示图片"} name="pic">
        <Switch />
      </Form.Item>
      <Form.Item label={null}>
        <Button type="primary" onClick={submit}>
          保存
        </Button>
      </Form.Item>
    </Form>
  )
}

function TGChannel() {
  const [form] = Form.useForm();
  const formItemLayout = {
    labelCol: { span: 6 },
    wrapperCol: { span: 18 },
  };
  const formItemLayoutWithOutLabel = {
    wrapperCol: { span: 18, offset: 6 },
  };
  const api = '/tgchannel/config'

  const submit = async () => {
    const data = await form.validateFields()
    console.log('data', data)
    try {
      await http.put(api, data)
      message.success('入库成功')
    } catch (e) {
      console.error(e)
      message.error(`入库失败：${e?.message}`)
    }
  }

  useEffect(() => {
    http.get(api)
      .then(data => {
        form.setFieldsValue(data)
      })
  }, [])

  return (
    <Form form={form} {...formItemLayout}>
      <Form.Item
        label={"TG域名"}
        name="url"
        rules={[
          {required: true, message: "请输入TG域名"}
        ]}
        extra={
        <div>
          需完整代理t.me域名，至少需要支持以下链接
          <p style={{margin: 0}}>1. 频道页面：https://t.me/s/xx，并且会带分页参数before=yy、搜索参数q=zz</p>
          <p style={{margin: 0}}>2. 消息详情页：https://t.me/xx/yy</p>
        </div>
        }
      >
        <Input style={{ width: '60%' }}/>
      </Form.Item>
      <h3>首页</h3>
      <Form.List
        name="homeChannelUsername"
        rules={[
          {
            required: true,
            message: '请添加频道'
          },
        ]}
      >
        {(fields, { add, remove }, { errors }) => (
          <>
            {fields.map((field, index) => (
              <Form.Item
                label={index === 0 ? '频道列表' : ''}
                required={false}
                key={field.key}
                {...(index === 0 ? formItemLayout : formItemLayoutWithOutLabel)}
                style={{marginBottom: 12}}
              >
                <Form.Item
                  {...field}
                  validateTrigger={['onChange', 'onBlur']}
                  rules={[
                    {
                      required: true,
                      whitespace: true,
                      message: "请输入频道名",
                    },
                  ]}
                  noStyle
                >
                  <Input placeholder="请输入频道名" style={{ width: '60%' }}/>
                </Form.Item>
                {fields.length > 1 ? (
                  <MinusCircleOutlined
                    className="dynamic-delete-button"
                    onClick={() => remove(field.name)}
                  />
                ) : null}
              </Form.Item>
            ))}
            <Form.Item label={''} {...formItemLayoutWithOutLabel}>
              <Button
                type="dashed"
                onClick={() => add()}
                style={{ width: '60%' }}
                icon={<PlusOutlined />}
              >
                添加频道
              </Button>
              <Form.ErrorList errors={errors} />
            </Form.Item>
          </>
        )}
      </Form.List>
      <h3>搜索</h3>
      <Form.Item label={"单频道资源数量"} name="count">
        <InputNumber min={1}/>
      </Form.Item>
      <Form.List
        name="channelUsername"
        rules={[
          {
            required: true,
            message: '请添加频道'
          },
        ]}
      >
        {(fields, { add, remove }, { errors }) => (
          <>
            {fields.map((field, index) => (
              <Form.Item
                label={index === 0 ? '频道列表' : ''}
                required={false}
                key={field.key}
                {...(index === 0 ? formItemLayout : formItemLayoutWithOutLabel)}
                style={{marginBottom: 12}}
              >
                <Form.Item
                  {...field}
                  validateTrigger={['onChange', 'onBlur']}
                  rules={[
                    {
                      required: true,
                      whitespace: true,
                      message: "请输入频道名",
                    },
                  ]}
                  noStyle
                >
                  <Input placeholder="请输入频道名" style={{ width: '60%' }}/>
                </Form.Item>
                {fields.length > 1 ? (
                  <MinusCircleOutlined
                    className="dynamic-delete-button"
                    onClick={() => remove(field.name)}
                  />
                ) : null}
              </Form.Item>
            ))}
            <Form.Item label={''} {...formItemLayoutWithOutLabel}>
              <Button
                type="dashed"
                onClick={() => add()}
                style={{ width: '60%' }}
                icon={<PlusOutlined />}
              >
                添加频道
              </Button>
              <Form.ErrorList errors={errors} />
            </Form.Item>
          </>
        )}
      </Form.List>
      <Form.Item label={null}>
        <Button type="primary" onClick={submit}>
          保存
        </Button>
      </Form.Item>
    </Form>
  )
}

function UCUt() {
  const [ut, setUt] = React.useState('');
  const api = '/uc/ut'

  const save = async () => {
    try {
      await http.put(api, {
        ut
      })
      message.success('设置成功')
    } catch (e) {
      console.error(e);
      message.error(`设置失败：${e?.message}`)
    }
  }

  useEffect(() => {
    http.get(api)
      .then(data => {
        setUt(data);
      })
  }, [])

  return (
    <div>
      <Alert message="关注微信公众号“王二小放牛娃”，输入“机器码”即可获得" type="info" style={{marginBottom: 16}}/>
      <Input.TextArea
        placeholder={`请输入UC机器码`}
        value={ut}
        onChange={(e) => setUt(e.target.value)}
        rows={4}
      />
      <Button type="primary" onClick={save} style={{marginTop: 16}}>保存</Button>
    </div>
  )
}

const TableRow = (props) => {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id: props['data-row-key'],
  });

  const style = {
    ...props.style,
    transform: CSS.Translate.toString(transform),
    transition,
    cursor: 'move',
    ...(isDragging ? { position: 'relative', zIndex: 9999 } : {}),
  };

  return <tr {...props} ref={setNodeRef} style={style} {...attributes} {...listeners} />;
};

function Sites() {
  const [dataSource, setDataSource] = useState([])
  const columns = useMemo(() => {
    return [
      {
        title: '站源',
        dataIndex: 'key',
        render(value, record) {
          return (
            <>
              {record.name}
              {record.t4 && <Tag style={{marginLeft: 4}} color="blue">T4</Tag>}
              {record.cms && <Tag style={{marginLeft: 4}} color="blue">CMS</Tag>}
            </>
          )
        }
      },
      {
        title: '是否启用',
        dataIndex: 'enable',
        render(value, record, index) {
          return (
            <Switch
              value={value}
              onChange={(checked) => {
                dataSource[index].enable = checked
                setDataSource([
                  ...dataSource
                ])
              }}
            />
          )
        },
        width: 120
      },
      {
        title: '操作',
        dataIndex: 'op',
        render(value, record, index) {
          return (
            <>
              <Button
                icon={<UpOutlined />}
                style={{marginRight: 8}}
                onClick={() => {
                  setDataSource((prev) => {
                    return arrayMove(prev, index, index - 1);
                  });
                }}
              />
              <Button
                icon={<DownOutlined />}
                onClick={() => {
                  setDataSource((prev) => {
                    return arrayMove(prev, index, index + 1);
                  });
                }}
              />
            </>
          )
        },
        width: 120
      },
    ]
  }, [dataSource]);

  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: {
        // https://docs.dndkit.com/api-documentation/sensors/pointer#activation-constraints
        distance: 1,
      },
    }),
  );

  const onDragEnd = ({ active, over }) => {
    if (active.id !== over?.id) {
      setDataSource((prev) => {
        const activeIndex = prev.findIndex((i) => i.key === active.id);
        const overIndex = prev.findIndex((i) => i.key === over?.id);
        return arrayMove(prev, activeIndex, overIndex);
      });
    }
  };

  const init = async () => {
    const fullConfig = await axios.get('/full-config')
    const allSites = fullConfig.data.video.sites

    const sites = await http.get('/sites/list')

    const visitedMap = {}
    const allSitesMap = {}
    allSites.forEach(site => {
      allSitesMap[site.key] = site
    })
    // 旧的取出来 过滤掉已失效的
    const rs = sites.filter(site => {
      visitedMap[site.key] = true
      return allSitesMap[site.key]
    })
    // 如果有新的站源 则追加到后面 默认启用
    allSites.forEach(site => {
      if (!visitedMap[site.key]) {
        rs.push({
          key: site.key,
          name: site.name,
          enable: true,
          t4: site.t4,
          cms: site.cms,
        })
      }
    })
    console.log('dataSource', rs, sites, allSites)
    setDataSource(rs)
  }

  const save = async () => {
    try {
      await http.put('/sites/list', {
        list: dataSource
      })
      message.success({
        content: (
          <>
            设置成功<br/>
            需重新加载源！<br/>
            需重新加载源！<br/>
            需重新加载源！<br/>
          </>
        ),
        duration: 5
      })
    } catch (e) {
      console.error(e);
      message.error(`设置失败：${e?.message}`)
    }
  }

  const reset = async () => {
    try {
      await http.delete('/sites/list')
      init()
      message.success('重置成功')
    } catch (e) {
      console.error(e);
      message.error(`重置失败：${e?.message}`)
    }
  }

  useEffect(() => {
    init()
  }, []);

  return (
    <div>
      <DndContext sensors={sensors} modifiers={[restrictToVerticalAxis]} onDragEnd={onDragEnd}>
        <SortableContext
          items={dataSource.map((i) => i.key)}
          strategy={verticalListSortingStrategy}
        >
          <Table
            components={{
            body: { row: TableRow },
          }}
            rowKey="key"
            columns={columns}
            dataSource={dataSource}
            pagination={false}
            className={'sites-table'}
          />
        </SortableContext>
      </DndContext>
      <Button type="primary" style={{marginTop: 16}} onClick={save}>保存</Button>
      <Button danger style={{marginTop: 16, marginLeft: 16}} onClick={reset}>重置</Button>
    </div>
  )
}

function Pans() {
  const [dataSource, setDataSource] = useState([])
  const columns = useMemo(() => {
    return [
      {
        title: '网盘',
        dataIndex: 'key',
        render(value, record, index) {
          return <Input value={record.name} onChange={(e) => {
            dataSource[index] = {
              ...record,
              name: e.target.value,
            }
            setDataSource([...dataSource])
          }}/>
        },
      },
      {
        title: '是否启用',
        dataIndex: 'enable',
        render(value, record, index) {
          return (
            <Switch
              value={value}
              onChange={(checked) => {
                dataSource[index].enable = checked
                setDataSource([
                  ...dataSource
                ])
              }}
            />
          )
        },
        width: 120
      },
      {
        title: '操作',
        dataIndex: 'op',
        render(value, record, index) {
          return (
            <>
              <Button
                icon={<UpOutlined />}
                style={{marginRight: 8}}
                onClick={() => {
                  setDataSource((prev) => {
                    return arrayMove(prev, index, index - 1);
                  });
                }}
              />
              <Button
                icon={<DownOutlined />}
                onClick={() => {
                  setDataSource((prev) => {
                    return arrayMove(prev, index, index + 1);
                  });
                }}
              />
            </>
          )
        },
        width: 120
      },
    ]
  }, [dataSource]);

  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: {
        // https://docs.dndkit.com/api-documentation/sensors/pointer#activation-constraints
        distance: 1,
      },
    }),
  );

  const onDragEnd = ({ active, over }) => {
    if (active.id !== over?.id) {
      setDataSource((prev) => {
        const activeIndex = prev.findIndex((i) => i.key === active.id);
        const overIndex = prev.findIndex((i) => i.key === over?.id);
        return arrayMove(prev, activeIndex, overIndex);
      });
    }
  };

  const init = async () => {
    const pans = await http.get('/pans/list')
    setDataSource(pans)
  }

  const save = async () => {
    try {
      await http.put('/pans/list', {
        list: dataSource
      })
      message.success('设置成功')
    } catch (e) {
      console.error(e);
      message.error(`设置失败：${e?.message}`)
    }
  }

  const reset = async () => {
    try {
      await http.delete('/pans/list')
      init()
      message.success('重置成功')
    } catch (e) {
      console.error(e);
      message.error(`重置失败：${e?.message}`)
    }
  }

  useEffect(() => {
    init()
  }, []);

  return (
    <div>
      <DndContext sensors={sensors} modifiers={[restrictToVerticalAxis]} onDragEnd={onDragEnd}>
        <SortableContext
          items={dataSource.map((i) => i.key)}
          strategy={verticalListSortingStrategy}
        >
          <Table
            components={{
              body: { row: TableRow },
            }}
            rowKey="key"
            columns={columns}
            dataSource={dataSource}
            pagination={false}
          />
        </SortableContext>
      </DndContext>
      <Button type="primary" style={{marginTop: 16}} onClick={save}>保存</Button>
      <Button danger style={{marginTop: 16, marginLeft: 16}} onClick={reset}>重置</Button>
    </div>
  )
}

function DanmuSetting() {
  const [form] = Form.useForm();
  const formItemLayout = {
    labelCol: { span: 4 },
    wrapperCol: { span: 20 },
  };
  const formItemLayoutWithOutLabel = {
    wrapperCol: { span: 20, offset: 4 },
  };
  const api = '/danmu/setting'

  const init = async () => {
    const data = await http.get(api)
    form.setFieldsValue(data)
  }

  const submit = async () => {
    const data = await form.validateFields()
    try {
      await http.put(api, data)
      message.success('入库成功')
    } catch (e) {
      console.error(e)
      message.error(`入库失败：${e?.message}`)
    }
  }

  const reset = async () => {
    try {
      await http.delete(api)
      init()
      message.success('重置成功')
    } catch (e) {
      console.error(e);
      message.error(`重置失败：${e?.message}`)
    }
  }

  useEffect(() => {
    init()
  }, [])

  return (
    <Form form={form} {...formItemLayout}>
      <Alert message="开启自动推送时，自动选取最快响应的API" type="info" style={{ marginBottom: 12 }}/>
      <Form.List
        name="urls"
        rules={[
          {
            required: true,
            message: '请添加API地址'
          },
        ]}
      >
        {(fields, { add, remove }, { errors }) => (
          <>
            {fields.map((field, index) => (
              <Form.Item
                label={index === 0 ? 'API列表' : ''}
                required={false}
                key={field.key}
                {...(index === 0 ? formItemLayout : formItemLayoutWithOutLabel)}
                style={{marginBottom: 12}}
              >
                <Form.Item
                  {...field}
                  name={[field.name, 'address']}
                  validateTrigger={['onChange', 'onBlur']}
                  rules={[
                    {
                      required: true,
                      whitespace: true,
                      message: `请输入API地址`,
                    },
                  ]}
                  noStyle
                >
                  <Input placeholder={`请输入API地址`} style={{ width: '65%' }}/>
                </Form.Item>
                <Form.Item
                  {...field}
                  name={[field.name, 'name']}
                  validateTrigger={['onChange', 'onBlur']}
                  noStyle
                >
                  <Input placeholder={`别名`} style={{ width: '25%', marginLeft: 8 }}/>
                </Form.Item>
                {fields.length > 1 ? (
                  <MinusCircleOutlined
                    className="dynamic-delete-button"
                    onClick={() => remove(field.name)}
                  />
                ) : null}
              </Form.Item>
            ))}
            <Form.Item label={''} {...formItemLayoutWithOutLabel}>
              <Button
                type="dashed"
                onClick={() => add()}
                style={{ width: '60%' }}
                icon={<PlusOutlined />}
              >
                添加API
              </Button>
              <Form.ErrorList errors={errors} />
            </Form.Item>
          </>
        )}
      </Form.List>
      <Form.Item label={"自动推送"} name="autoPush">
        <Switch/>
      </Form.Item>
      <Form.Item label={null}>
        <Button type="primary" onClick={submit}>
          保存
        </Button>
        <Button danger style={{marginLeft: 16}} onClick={reset}>重置</Button>
      </Form.Item>
    </Form>
  )
}

function T4() {
  const [form] = Form.useForm();
  const formItemLayout = {
    labelCol: { span: 0 },
    wrapperCol: { span: 24 },
  };
  const formItemLayoutWithOutLabel = {
    wrapperCol: { span: 24, offset: 0 },
  };
  const api = '/t4/list'

  const init = async () => {
    const list = await http.get(api)
    form.setFieldsValue({list})
  }

  const submit = async () => {
    const data = await form.validateFields()
    try {
      await http.put(api, data.list)
      message.success({
        content: (
          <>
            保存成功<br/>
            需重新加载源！<br/>
            需重新加载源！<br/>
            需重新加载源！<br/>
          </>
        ),
        duration: 5
      })
    } catch (e) {
      console.error(e)
      message.error(`保存失败：${e?.message}`)
    }
  }

  const reset = async () => {
    try {
      await http.delete(api)
      init()
      message.success('清空成功')
    } catch (e) {
      console.error(e);
      message.error(`清空失败：${e?.message}`)
    }
  }

  useEffect(() => {
    init()
  }, [])

  return (
    <Form form={form} {...formItemLayout}>
      <Form.List name="list">
        {(fields, { add, remove }, { errors }) => (
          <>
            {fields.map((field, index) => (
              <Form.Item
                required={false}
                key={field.key}
                {...formItemLayout}
                style={{marginBottom: 12}}
              >
                <Form.Item
                  {...field}
                  name={[field.name, 'address']}
                  validateTrigger={['onChange', 'onBlur']}
                  rules={[
                    {
                      required: true,
                      whitespace: true,
                      message: `请输入T4接口地址`,
                    },
                  ]}
                  noStyle
                >
                  <Input placeholder={`请输入T4接口地址`} style={{ width: '65%' }}/>
                </Form.Item>
                <Form.Item
                  {...field}
                  name={[field.name, 'name']}
                  validateTrigger={['onChange', 'onBlur']}
                  noStyle
                  rules={[
                    {
                      required: true,
                      whitespace: true,
                      message: `请输入名称`,
                    },
                  ]}
                >
                  <Input placeholder={`名称`} style={{ width: '25%', marginLeft: 8 }}/>
                </Form.Item>
                <MinusCircleOutlined
                  className="dynamic-delete-button"
                  onClick={() => remove(field.name)}
                />
              </Form.Item>
            ))}
            <Form.Item label={''} {...formItemLayoutWithOutLabel}>
              <Button
                type="dashed"
                onClick={() => add()}
                style={{ width: '60%' }}
                icon={<PlusOutlined />}
              >
                添加T4接口
              </Button>
              <Form.ErrorList errors={errors} />
            </Form.Item>
          </>
        )}
      </Form.List>
      <Form.Item label={null}>
        <Button type="primary" onClick={submit}>
          保存
        </Button>
        <Button danger style={{marginLeft: 16}} onClick={reset}>清空</Button>
      </Form.Item>
    </Form>
  )
}

function CMS() {
  const api = '/cms/list'
  const [url, setUrl] = useState('')
  const [data, setData] = useState([])
  const [getDataLoading, setGetDataLoading] = useState(false)
  const columns = [
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: 'API',
      dataIndex: 'address',
      key: 'address',
    },
  ]

  const init = async () => {
    const list = await http.get(api)
    setData(list)
  }

  const getData = async () => {
    if (!url) {
      message.error("请输入CMS接口地址")
      return
    }
    setGetDataLoading(true)
    try {
      const {data} = await axios.get(url.trim())
      if (data.sites.length) {
        setData(
          data.sites.map(item => {
            return {
              name: item.name,
              address: item.api,
            }
          })
        )
      }
    } catch (e) {
      console.error(e)
      message.error("接口解析失败")
    }
    setGetDataLoading(false)
  }

  const save = async () => {
    try {
      await http.put(api, data)
      message.success({
        content: (
          <>
            保存成功<br/>
            需重新加载源！<br/>
            需重新加载源！<br/>
            需重新加载源！<br/>
          </>
        ),
        duration: 5
      })
    } catch (e) {
      console.error(e)
      message.error(`保存失败：${e?.message}`)
    }
  }

  const reset = async () => {
    try {
      await http.delete(api)
      init()
      message.success('清空成功')
    } catch (e) {
      console.error(e);
      message.error(`清空失败：${e?.message}`)
    }
  }

  useEffect(() => {
    init()
  }, [])

  return (
    <Form>
      <Space.Compact style={{ width: '100%', marginBottom: 12 }}>
        <Input defaultValue="输入XPTV CMS接口地址" value={url} onChange={(e) => setUrl(e.target.value)} />
        <Button type="primary" onClick={getData} loading={getDataLoading}>识别</Button>
      </Space.Compact>
      <Table columns={columns} dataSource={data} pagination={false}/>
      <Form.Item label={null} style={{marginTop: 16}}>
        <Button type="primary" onClick={save}>
          保存
        </Button>
        <Button danger style={{marginLeft: 16}} onClick={reset}>清空</Button>
      </Form.Item>
    </Form>
  )
}

function App() {
  const props = {
    accept: ".json",
    showUploadList: false,
    beforeUpload(file) {
      const reader = new FileReader();
      reader.onload = async (e) => {
        try {
          const json = JSON.parse(e.target.result);
          console.log("Parsed JSON data:", json);
          await http.put('/backup', json);
          message.success(`${file.name}导入成功`);
        } catch (err) {
          message.error(`${file.name}导入失败: ${err.message}`);
          console.error('Error parsing JSON:', err);
        }
      };
      reader.readAsText(file);
      return false;
    }
  };

  return (
    <div className={'container'}>
      <Card style={{ minHeight: 600, maxHeight: '100vh', width: 600 }}>
        <Tabs
          type="card"
          tabBarExtraContent={<div style={{display: 'flex', alignItems: 'center'}}>
            <Upload {...props}>
              <Button type={"link"}>导入</Button>
            </Upload>
            <a href={'/website/backup'} target={"_blank"}>导出</a>
          </div>}
        >
          <TabPane tab="登录信息" key="account">
            <Tabs>
              <TabPane tab="夸克" key="quark">
                <QrcodeCard
                  qrcodeUrl="/website/quark/qrcode"
                  cacheUrl="/quark/cookie"
                />
              </TabPane>
              <TabPane tab="UC Cookie" key="uc-cookie">
                <QrcodeCard
                  qrcodeUrl="/website/uc/qrcode"
                  cacheUrl="/uc/cookie"
                />
              </TabPane>
              <TabPane tab="UC token" key="uc-token">
                <QrcodeCard
                  qrcodeUrl="/website/uc-tv/qrcode"
                  cacheUrl="/uc-tv/token"
                />
              </TabPane>
              <TabPane tab="UC机器码" key="uc-ut">
                <UCUt/>
              </TabPane>
              <TabPane tab="115" key="115">
                <QrcodeCard
                  qrcodeUrl="/website/115/qrcode"
                  cacheUrl="/115/cookie"
                />
              </TabPane>
              <TabPane tab="百度" key="baidu">
                <QrcodeCard
                  qrcodeUrl="/website/baidu/qrcode"
                  cacheUrl="/baidu/cookie"
                />
              </TabPane>
              <TabPane tab="阿里" key="ali">
                <AliQrcodeCard />
              </TabPane>
              <TabPane tab="天翼" key="tianyi">
                <AccountInfo api="/tianyi/account"/>
              </TabPane>
              <TabPane tab="123" key="123">
                <AccountInfo api="/pan123/account"/>
              </TabPane>
            </Tabs>
          </TabPane>
          <TabPane tab="站源设置" key="site">
            <Tabs destroyOnHidden={true}>
              <TabPane tab="站源列表" key="sites">
                <Sites/>
              </TabPane>
              <TabPane tab="T4接口" key="t4">
                <T4/>
              </TabPane>
              <TabPane tab="XPTV-CMS接口" key="cms">
                <CMS/>
              </TabPane>
              <TabPane tab="木偶域名" key="muou">
                <SiteDomainSetting api={'/muou/urls'} name="木偶"/>
              </TabPane>
              <TabPane tab="玩偶域名" key="wogg">
                <SiteDomainSetting api={'/wogg/urls'} name="玩偶"/>
              </TabPane>
              <TabPane tab="雷鲸域名" key="leijing">
                <SiteDomainSetting api={'/leijing/urls'} name="雷鲸"/>
              </TabPane>
              <TabPane tab="至臻域名" key="zhizhen">
                <SiteDomainSetting api={'/zhizhen/urls'} name="至臻"/>
              </TabPane>
              <TabPane tab="TG频道" key="tgchannel">
                <TGChannel/>
              </TabPane>
              <TabPane tab="TG搜" key="tgsou">
                <TGSou/>
              </TabPane>
            </Tabs>
          </TabPane>
          <TabPane tab="通用设置" key="common">
            <Tabs>
              <TabPane tab="网盘列表" key="pans">
                <Pans/>
              </TabPane>
              <TabPane tab="弹幕设置" key="danmu">
                <DanmuSetting/>
              </TabPane>
            </Tabs>
          </TabPane>
        </Tabs>
      </Card>
    </div>
  )
}

export function renderClient() {
  const root = createRoot(document.getElementById('app'));
  root.render(<App/>);
}
