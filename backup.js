const url = "https://4443e105ad4b.ngrok.io";
const fetch = require('node-fetch');
const Path = require('path')

exports.helloWorld = async (req, res) => {
  const path = req.path.split('/')

  switch (path[1]) {
    case 'cell':
      cellAction(req, res, path);
      break;
    case 'query':
      if (path[path.length-1]==='form') {
        res.send({
          fields: [{
            name: 'tag', label: 'Your Cool Tag Name'
          }]
        })
      } else {
        await queryAction(req, res, path);
      }
      break;
    default:
      res.send(actionList());
  }
};



function actionList () {
  return {
    "label": "Action API Demo",
    "integrations": [
      {
        name: "query_demo",
        label: "Action API Demo (Query)",
        supported_action_types: ["query"],
        supported_formats: ['json'],
        supported_formattings: ['unformatted'],
        supported_visualization_formattings: ['noapply'],
        supported_download_settings: ['push'],
        url: `${url}/query`,
        form_url: `${url}/query/form`
      },
      {
        name: "cell_demo_low",
        label: "Action API Demo (Cell Low)",
        supported_action_types: ["cell"],
        required_fields: [{"tag": "threshold_low" }],
        url: `${url}/cell/low`,
        form_url: `${url}/cell/low/form`
      },
      {
        name: "cell_demo_medium",
        label: "Action API Demo (Cell Medium)",
        supported_action_types: ["cell"],
        required_fields: [{"tag": "threshold_medium" }],
        url: `${url}/cell/medium`,
        form_url: `${url}/cell/medium/form`
      },
      {
        name: "cell_demo_high",
        label: "Action API Demo (Cell High)",
        supported_action_types: ["cell"],
        required_fields: [{"tag": "threshold_high" }],
        url: `${url}/cell/high`,
        form_url: `${url}/cell/high/form`
      }
    ]
  }
}

async function cellAction (req, res, path) {
  const { body } = req;
  const { data } = body;
  switch (path[path.length-1]) {
    case 'form':
      res.send({
        fields: [
          {
            name: 'value',
            label: `Input a value for ${path[path.length-2]}`,
            types: 'text',
            default: data.value
          }
        ]
      })
      break;
    default:
      await insertRow(path[path.length-1], body.form_params.value )
      res.send({
        "looker": {
          "success": true,
          "refresh_query": true
        }
      })
  }
}

async function insertRow (field, value) {
  const SQL = `
INSERT INTO looker-private-demo.looker_scratch.zz_action_api
VALUES ( CAST({{ _user_attributes['id'] }} as INT64), '${field}', ${value}, CURRENT_TIMESTAMP() ) 
  `
  const { LookerNodeSDK } = require('@looker/sdk-node')
  const sdk = LookerNodeSDK.init40()
  const create = await sdk.ok(sdk.create_sql_query({
    model_name: 'action_api',
    sql: SQL
  }))
  return sdk.ok(sdk.run_sql_query(create.slug, 'json'));
}

async function queryAction (req, res, path) {
  const query = req.body.scheduled_plan.query;
  const { LookerNodeSDK } = require('@looker/sdk-node')
  const sdk = LookerNodeSDK.init40()
  const inline = await sdk.ok(sdk.run_inline_query({
    body: {
      model: query.model,
      view: query.view,
      fields: ['order_items.user_id'],
      filters: query.filters,
      filter_expression: query.filter_expression
    },
    result_format: 'sql',
    limit: -1
  }))
  const create = await sdk.ok(sdk.create_sql_query({
    model_name: 'action_api',
    sql: `
INSERT INTO looker-private-demo.looker_scratch.zz_action_api_tags (user_id, looker_user_id, tag, inserted_at)
SELECT 
  *, 
  CAST({{ _user_attributes['id'] }} as INT64), 
  '${req.body.form_params.tag}' as tag, 
  CURRENT_TIMESTAMP() as inserted_at
FROM (
  ${inline.replace('LIMIT 100000', '')}
)
    `
  }))
  await sdk.ok(sdk.run_sql_query(create.slug, 'json'));
  res.send({
    "looker": {
      "success": true,
      "refresh_query": true
    }
  })
}
