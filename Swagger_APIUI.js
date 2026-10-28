const formidable = require('formidable'); //處裡檔案上傳
const path = require('path'); // 用來取得檔名及路徑
const urlencode = require('urlencode'); // 轉換檔案格式，避免非英文檔名在下載時出現亂碼
const express = require('express');
const swaggerUi = require('swagger-ui-express');
const { randomUUID } = require('crypto');
const crypto = require("crypto");
const querystring = require('querystring');
const { WebSocketServer } = require('ws'); 
const WebSocket = require('ws');
const app = express();
const multer = require('multer');
const upload = multer();

// 自建模組
const config = require('./config/config.js');
const { formattedTime } = require('./config/formatDateTime.js'); // 標準化時間格式

//解析 yaml 檔
const fs = require('fs');
const YAML = require('yaml');
const file = fs.readFileSync(path.join(__dirname, 'CHY_API.yaml'), 'utf8'); 
const swaggerDocument = YAML.parse(file); 

app.use(express.json()); // 處理request-body資料
app.use(express.urlencoded({ extended: false }));

app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument));

//載入 JWT 模組
const jwt = require('jsonwebtoken');
const bcrypt = require('bcrypt');
const { createTokens, authenticateToken } = require('./config/jwt.js');

// 載入 mysql 模組
// const mysql = require('mysql');
const mysql = require('mysql2');

// 載入cors模組，當SWagger API發生fetch error時需加入cors
// 當發起一次GET/DELETE請求時，參數會以url string的形式進行傳遞。 即?後的字串則為其請求參數，並以&作為分隔符號。//
// e.g, http://localhost:8880/api/chy/user/?userId=0'
// 當發起一次POST請求時，參數則會以 body形式傳遞
var cors = require('cors');
const corsOptions = { // ** 還要考慮到 外網的 api設置 ** 
	origin: ['http://54.253.130.202', 'http://localhost:8880', 'http://192.168.10.12', 'http://localhost:8886', 'http://localhost:7777','http://118.163.229.160:8886', 'http://118.163.229.160:80'],
	methods: 'GET, HEAD, PUT, PATCH, POST, DELETE, OPTIONS',
	allowedHeaders: ['Content-Type', 'Authorization'], 
};
app.use(cors(corsOptions));

// 資料庫連線資訊
const connectionPool = mysql.createPool({
	host: config.mysqlHost,
	//path: config.myPath,
	user: config.mysqlUserName,
	password: config.mysqlPass,
	database: config.mysqlDatabase,
	connectionLimit: config.mysqlConnectLimit,
	// supportBigNumbers: true,  // mysql2 啟用大數支援
  	// bigNumberStrings: false 
});

// websocket 紀錄連線使用者
var clientMap = new Map(); 

// 載入 AWS S3的模組
// var AWS = require('aws-sdk'); 
const {
	S3Client,
	PutObjectCommand,
	DeleteObjectCommand,
	HeadObjectCommand,
	GetObjectCommand,
} = require('@aws-sdk/client-s3');
const { Console, timeStamp } = require('console');
const { APIGateway } = require('aws-sdk');
const { queryObjects } = require('v8');
const { workerData } = require('worker_threads');
const { zstdCompress } = require('zlib');
const e = require('express');

// S3 設定區域及連線資訊
BUCKET_REGION = config.S3_BUCKET_REGION;
BUCKET_NAME = config.BUCKET_NAME;
AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID;
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY;

// 建立 S3 用戶端實例
const s3Client = new S3Client({
	region: config.S3_BUCKET_REGION,
	credentials: {
		accessKeyId: config.AWS_ACCESS_KEY_ID,
		secretAccessKey: config.AWS_SECRET_ACCESS_KEY,
		signatureVersion: 'v4',
	},
});

// 信用卡付款操作
const Operation = Object.freeze({
            buySW: 0,
            subSW: 1,
            termSubSW: 2,
			termSubSuc: 3,
            cancelAuth: 4,
            cancelAuthSuc: 5,
            singleQuery: 6,
        });

// 使用者付款狀態
const userStatus = Object.freeze({
	terminate: 0,  // 停止
	activate: 1,   // 啟用
	pay: 2,        // 付費
	sub: 3,        // 訂閱
	unSub: 4,      // 解除訂閱(還在訂閱期限內) 
});

// 載入 user_api_route 路由
// const usersRouter = require('./routes/user_api_route.js');
// app.use('/api/chy/', usersRouter);

/*
    register user ，登入使用username當帳號，所以額外判斷username有沒有重複
*/
app.post('/api/chy/user/register', function (req, res) {
	console.log('Register new user'); 

	connectionPool.getConnection((connectionError, connection) => {
		// 資料庫連線
		if (connectionError) {
			res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
		} else {
			//檢查req.body是不是7個 {username, phone, password, lastName, firstName, email, userStatus}
			if (Object.keys(req.body).length != 7) {
				return res.send({
					code: 200,
					message: 'The field data of the transmission cannot be empty.',
				});
			}

			const { username, firstName, lastName, email, password, phone, userStatus } = req.body;

			let outcome; 
			outcome = ifObjectLengthQualify(req.body, ['username', 'password'], [6, 8]);
			if(outcome.hasErr) 
				return res.send({ code: 200, message: outcome }); 

			// 判斷 req.body 裡的 欄位數值 是不是空字串 
			outcome = ifObjectItemIsEmpty(req.body); 
			if (outcome.hasErr) 
				return res.send({ code: 200, message: outcome.message }); 
			
			// 檢查 req.body 物件中的 username、password 的 value 不能有空格
			outcome = ifObjItemSpace(req.body, ['username', 'password']); 
			if (outcome.hasErr) 
				return res.send({ code: 200, message: outcome.message });
			

			// queryStatement~加入資料user資料，並檢查username有無重複
			const queryStatement =
				'INSERT INTO user_info (username, phone, password, lastName, firstName, email, userStatus, authTime, endDate) SELECT ?,?,?,?,?,?,? FROM DUAL WHERE NOT EXISTS(SELECT 1 FROM user_info WHERE username = ?)';
			connection.query(
				queryStatement,
				[username, phone, password, lastName, firstName, email, userStatus, username, 0, "0"],
				(err, result) => {
					// 執行資料庫的查詢，result為查詢結果。err為錯誤訊息的回報，如果有錯誤的話

					if (err) {
						// err 1.email重複 / 2.網路問題 / 3.mysql server stop /  4.else...
						if (err.code == 'ER_DUP_ENTRY') {
							res.send({
								code: 200,
								message: err.sqlMessage.replace('user_info.email', 'email'),
							});
						} else res.send({ code: 200, error: err });
					} else if (result.affectedRows == 0) {
						// result.affectedRows回傳0代表資料庫沒有新增資料，可能是之前的queryStatement判斷username重複
						res.send({
							code: 200,
							message: 'Duplicate username and Registration Failed',
							error: err,
						});
					} else {
						res.send({ code: 200, message: 'Successfully added new users' });
					}
					connection.release(); // 釋放連線
				}
			);
		}
	});
});

/*
    User login
*/
app.post('/api/chy/user/login', function (req, res) {
	console.log('User login');

	connectionPool.getConnection((connectionError, connection) => {
		// 資料庫連線
		if (connectionError) {
			res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
		} else {
			// queryStatement，找password和user_id
			const queryStatement = 'SELECT password, user_id, userStatus FROM user_info WHERE username = ?';
			connection.query(queryStatement, [req.body.username], (err, result) => {
				if (err) {
					res.send({ code: 200, message: 'SQL ERROR', error: err });
				} else if (Object.keys(result).length === 0) { 
					res.send({ code: 200, message: 'Account has not been registered' }); // username尚未註冊
				} else {
					//密碼驗證，這裡沒有加密，純比較
					if (result[0]['password'] == req.body.password) {
						// 產生JWT，設定在./config/jwt.js及./config/config.js
						const accessToken = createTokens(req.body.username, result[0]['user_id']);
						res.send({
							code: 200,
							message: 'Login Successfully',
							userStatus: result[0]['userStatus'],
							data: [
								{
									access_token: accessToken,
									token_type: config.myJWTtokenType,
									expires_in: config.myJWTexpiresIn,
								},
							],
						});
					} else {
						res.send({ code: 200, message: 'error password' });
					}
				}
				connection.release(); // 釋放連線
			});
		}
	});
});

/*
    Get user by userId
*/
// app.get('/api/chy/user/', function(req, res) {

//   console.log("user get user_id query "+req.query.user_id);

//   connectionPool.getConnection((connectionError, connection) => { // 資料庫連線
//     if(connectionError){
//       res.send({"code":200,"message":connectionError});// mysql連線有問題回傳錯誤
//     }else{
//       // queryStatement
//       const queryStatement = "SELECT * FROM user_info WHERE user_id = ?"
//       connection.query(queryStatement, [req.query.user_id],
//         (err, result) => {
//           if (result.length == 0) { // query結果沒有找到user_id
//             res.send({"code":200,"message":"No this user_id"});
//           } else {
//             // result是物件格式，轉換為json格式，並且將result換名為data，因為API上的名稱是data
//             data = JSON.stringify(result);
//             data = JSON.parse(data)
//             res.send({"code":200,
//                       "message":"Successfully query user_id: "+req.query.user_id,
//                        data});
//           }
//           connection.release(); // 釋放連線
//         }
//       )
//     }
//   })
// });

/*
    Get user by userId in JWT
*/
app.get('/api/chy/user/', function (req, res) {

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
	
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => { // JWT驗證 
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功~更新user_info資料表
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					const { serialNumber, device_id, deviceName } = req.body;
					const queryStatement = 'SELECT * FROM user_info WHERE user_id = ?';
					
					connection.query(queryStatement, [payload.user_id], (err, result) => {
						if (Object.keys(result).length === 0) {
							res.send({ code: 200, message: 'The user id was not found.' });
							console.log('The user id was not found');
						} else {
							data = JSON.stringify(result); // 轉換為JSON
							data = JSON.parse(data);
							res.send({
								code: 200,
								message: 'Successfully query user_id: ' + payload.user_id,
								data,
							});
						}
						connection.release(); // 釋放連線
					});
				}
			});
		}
	});
});

/* *********************************** */
// app.get('/api/chy/user/', authenticateToken,function(req, res) {

//   token = req.headers.authorization //取得header裡的token
//   if(typeof(req.headers.authorization) == "string" ){ //驗證headers有沒有帶jwt token，有的話分割bearer字串和token的value
//     token = req.headers.authorization.split(' ')[1];
//   }

//   connectionPool.getConnection((connectionError, connection) => { // 資料庫連線
//     if(connectionError){
//       res.send({"code":200,"message":connectionError}); // mysql連線有問題回傳錯誤
//     }else{
//       // 取得req.body資料{serialNumber, device_id, deviceName}
//       const{serialNumber, device_id, deviceName} = req.body;
//       const queryStatement = "SELECT * FROM user_info WHERE user_id = ?"
//       connection.query(queryStatement, [payload.user_id],
//         (err, result) => {
//           if (Object.keys(result).length ===0) {
//             res.send({"code":200, "message":"The user id was not found."});
//             console.log("The user id was not found")
//           } else {
//             data = JSON.stringify(result); // 轉換為JSON
//             data = JSON.parse(data)
//             res.send({"code":200,
//                       "message":"Successfully query user_id: "+req.user_id,
//                        data});
//           }
//           connection.release(); // 釋放連線
//         }
//       )
//     }
//   })
// });

/*
    update user information by user_id in JWT
*/
// patch更新部分，put全更新，更新user_info裡username = username的資料，除了user_id外都更新，user_id是整數自動遞增的主鍵不會去改動
app.put('/api/chy/user/', function (req, res) {
	console.log('Update user ' + req.body.username);
	const { firstName, lastName, email, password, phone, userStatus } = req.body;

	let outcome;
	// 判斷 req.body 裡的 欄位數值 是不是空字串
	outcome = ifObjectItemIsEmpty(req.body);
	if (outcome.hasErr) {
		return res.send({ code: 200, message: outcome.message });
	}

	// 檢查 req.body 物件中的 serialNumber、deviceName 的 value 不能有空格
	outcome = ifObjItemSpace(req.body, ['password']);
	if (outcome.hasErr) {
		return res.send({ code: 200, message: outcome.message });
	}

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1] 
						: null;

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功~更新user_info資料表
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement
					const queryStatement =
						'UPDATE user_info SET userStatus = ?, phone = ?, password = ?, lastName = ?, firstName = ?, email = ? WHERE user_id = ?';
					connection.query(
						queryStatement, 
						[userStatus, phone, password, lastName, firstName, email, payload.user_id],
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'Username does not exist!',
									error: err,
								});
							} else {
								res.send({
									code: 200,
									message:
										'username ' + payload.user_id + ' updated successfully!',
								});
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*
    delete this user by userid
*/
app.delete('/api/chy/user/', function (req, res) {
	console.log('user del query: ' + req.query.user_id);

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功~更新user_info資料表
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement
					const queryStatement = 'DELETE FROM user_info WHERE user_id = ?';
					connection.query(queryStatement, [payload.user_id], (err, result) => {
						if (err) {
							res.send({ code: 200, message: 'user_id does not exist!', error: err });
						} else {
							res.send({
								code: 200,
								message: 'user_id ' + req.query.user_id + ' delete successful!',
							});
						}
						connection.release(); // 釋放連線
					});
				}
			});
		}
	});
});

/*
    increasing new meter  (auto-incrementing device_id and add the meter's user_id in JWT)
*/
app.post('/api/chy/device', function (req, res) {
	console.log('Increasing new meter');

	// 格式化日期，./config/formatDate.js裡override toISOstring，T在ISO時間格式 表示 區分 日 和 時間
	const createTime = new Date().toISOString();
	let outcome;

	// 判斷 req.body 裡的 欄位數值 是不是空字串
	outcome = ifObjectItemIsEmpty(req.body);
	if (outcome.hasErr) {
		return res.send({ code: 200, message: outcome.message });
	}

	// 檢查 req.body 物件中的 serialNumber、deviceName 的 value 不能有空格
	outcome = ifObjItemSpace(req.body, ['serialNumber', 'deviceName']);
	if (outcome.hasErr) {
		return res.send({ code: 200, message: outcome.message });
	}

	const { serialNumber, device_id, deviceName, location, machine_type } = req.body;

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功~更新user_info資料表
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement~加入資料meter資料，並檢查serialNumber有無重複，user_id從JWT拿
					const queryStatement =
						'INSERT INTO device_info (serialNumber, device_id, deviceName, user_id, createTime,location, machine_type) SELECT ?,?,?,?,?,?,? FROM DUAL WHERE NOT EXISTS(SELECT 1 FROM device_info WHERE serialNumber = ? )';
					connection.query(
						queryStatement,
						[
							serialNumber,
							device_id,
							deviceName,
							payload.user_id,
							createTime,
							location,
							machine_type,
							serialNumber,
						],
						(err, result) => {
							if (err) {
								res.send({ code: 200, err: err });
							} else if (result.affectedRows == 0) {
								// result.affectedRows回傳0代表資料庫沒有新增資料，可能是之前的queryStatement判斷重複serialNumber
								res.send({
									code: 200,
									message: 'Duplicate serialNumber and failed to add new meter!',
								});
								console.log('Duplicate serialNumber and failed to add new meter!');
							} else {
								res.send({ code: 200, message: 'Successfully added new meter!' });
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*
    Get the device list by user_id in JWT.
    size代表一頁幾筆資料
    page代表從第幾頁開始
    sort按照時間排序， 使用 ISO Date Time Format 
    例如: size:50/page:0 代表從50*0筆~50*(0+1)筆的資料，size:50/page:3 為150~200筆資料
*/
app.get('/api/chy/device', function (req, res) {
	let { size, page, sort } = req.query; // 取得req.query資料
	sort = sort.replace(':', ' '); // 處理sort字串

	page = parseInt(page);
	size = parseInt(size);

	// 檢查 page>=0  和 size>=1
	if (!(page >= 0 && size >= 1 && typeof page == 'number' && typeof size == 'number')) {
		return res.send({
			code: 200,
			message: 'page must be >=0 and size must be >=1, all of them are numbers!',
		});
	}
	let front = page * size;
	let end = size;

	// limit 5,1 從第5條開始返回1條紀錄
	console.log('device get query from device_id ' + front + ' to ' + end);

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功~更新user_info資料表
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement
					const queryStatement =
						'SELECT serialNumber, device_id, deviceName, user_id, createTime, ,location, machine_type, FROM device_info WHERE user_id = ?  ORDER BY ? LIMIT ?,?';
					const query = `
								SELECT 
									di.device_id, 
									di.serialNumber, 
									di.deviceName, 
									di.user_id, 
									di.createTime, 
									di.location,
									di.machine_type,
									COUNT(dl.log_id) as quantity 
								FROM device_info di 
								LEFT JOIN device_log dl ON di.serialNumber = dl.serialNumber 
								WHERE di.user_id = ?
								GROUP BY di.device_id, di.serialNumber, di.deviceName, di.user_id, di.createTime, di.location, di.machine_type
								ORDER BY di.device_id    
								LIMIT ?,?      
								`; //
					connection.query(
						query,
						[payload.user_id, front, end], // limit 5,1 從第5條開始返回1條紀錄
						(err, result) => {
							if (err) {
								// SELECT在日期區間中沒找到資料
								res.send({ code: 200, message: err });
								console.log('No device found!');
							} else {
								res.send({ code: 200, data: result, message: 'Success!' });
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*
    Update new meter by userId in JWT
*/
app.put('/api/chy/device', function (req, res) {
	
	const { oldSerialNumber, serialNumber, deviceName, location, machine_type } = req.body;
	let outcome;
	console.log('Update new meter');

	// 判斷 req.body 裡的 欄位數值 是不是空字串
	outcome = ifObjectItemIsEmpty(req.body);
	if (outcome.hasErr) {
		return res.send({ code: 200, message: outcome.message });
	}

	// 檢查 req.body 物件中的 serialNumber、deviceName 的 value 不能有空格
	outcome = ifObjItemSpace(req.body, ['serialNumber', 'deviceName']);
	if (outcome.hasErr) {
		return res.send({ code: 200, message: outcome.message });
	}

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功~更新user_info資料表
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement
					const queryStatement =
						'UPDATE device_info SET serialNumber=?, deviceName=?, location=?, machine_type=? WHERE (user_id = ? and serialNumber =?)';
					connection.query(
						queryStatement,
						[
							serialNumber,
							deviceName,
							location,
							machine_type,
							payload.user_id,
							oldSerialNumber,
						],
						(err, result) => {
							if (err) {
								res.send({ code: 200, message: 'Update failed', error: err });
								console.log(err);
							} else {
								res.send({
									code: 200,
									message: 'Update device: ' + deviceName + ' successful!',
								});
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*
    Delete meter by deviceId and userId in JWT
*/
app.delete('/api/chy/device', function (req, res) {
	// 取得req.body資料
	const { device_id } = req.query;
	console.log('Delete the device ID of the meter as ' + device_id);

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功~更新user_info資料表
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement
					const queryStatement =
						'DELETE FROM device_info WHERE device_id = ? and user_id = ?';
					connection.query(
						queryStatement,
						[device_id, payload.user_id],
						(err, result) => {
							if (err) {
								res.send({ code: 200, err: err });
							} else if (result.affectedRows == 0) {
								//欲刪除的device_id不存在
								res.send({
									code: 200,
									message: 'The device_id to be deleted does not exist.',
								});
								console.log(err);
							} else {
								res.send({
									code: 200,
									message:
										'Delete device from deviceId ' + device_id + ' successful!',
								});
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*
    upload data from meter
*/
app.post('/api/chy/device/log', function (req, res) {
	console.log('Upload data from meter');

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功

			// 取得req.body資料，在API上的格式為[{...}, {...}]，讀進來為物件的陣列，處理並轉換成二維陣列，以利SQL使用，並加上JWT裡的user_id
			// 此外配合後面WHERE判斷，要插入user_log表格的serialNumber和device_id，是不是和user_info裡的的serialNumber和device_id，有沒有完全對應
			// 例如: 不可以出現(device_id, serialNumber)在user_info表格為(0, CHYTM1)，插入user_log表格時為(0, CHYTM2)
			// 所以再加上device_id和serialNumber的restriction

			let data = [];
			let serialNumberList = [];
			let user_idList = [];
			for (let i = 0; i < req.body.length; i += 1) {
				// 堆疊 device_log 檔案
				data.push(Object.values(req.body[i]).concat([payload.user_id]));
			}

			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// FROM DUAL WHERE EXISTS(SELECT 1 FROM device_info WHERE serialNumber = 'CHYTMs' AND user_id = 0)
					// queryStatement，user_id從JWT拿
					const queryStatement =
						'INSERT INTO device_log (unit, serialNumber, UpdateTime, T1minusT2, T1, T2, user_id) VALUES ?';
					const queryStatement2 =
						'INSERT INTO device_log (unit, serialNumber, UpdateTime, T1minusT2, T1, T2, user_id) VALUES ? FROM device_info d WHERE d.serialNumber = ?';
					connection.query(queryStatement, [data], (err, result) => {
						if (err) {
							if (err.errno == 1452) {
								res.send({
									code: 200,
									message:
										'Please add the serialNumber of the device to device page :)',
									error: err,
								});
								console.log('Failed to add meter log!');
							} else {
								res.send({
									code: 200,
									message: 'Failed to add meter log!',
									error: err,
								});
								console.log('Failed to add meter log!');
							}
						} else {
							res.send({ code: 200, message: 'Successfully add new meter log!' });
						}
						connection.release(); // 釋放連線
					});
				}
			});
		}
	});
});

/*
    get meter log
*/
app.get('/api/chy/device/log', function (req, res) {
	let { serialNumber, size, page, sort } = req.query; // 取得req.body資料
	let Count = 0; // 計算資料筆數
	sort = sort.replace(':', ' '); // 處理sort字串，UpdateTime:DESC => UpdateTime DESC

	// 傳進來的 page 和 size 如果是字串 拿去運算會產生問題
	page = parseInt(page);
	size = parseInt(size);

	// 檢查 page>=0  和 size>=1
	if (!(page >= 0 && size >= 1 && typeof page == 'number' && typeof size == 'number')) {
		return res.send({
			code: 200,
			message: 'page must be >=0 and size must be >=1, all of them are numbers!',
		});
	}
	let front = page * size;
	let end = front + size;

	console.log('get query from device_log ' + front + ' to ' + end);

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功

			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement，user_id從JWT拿
					const queryStatement = `SELECT unit, serialNumber, UpdateTime, T1minusT2, T1, T2 FROM device_log WHERE (user_id = ? AND serialNumber = ?) ORDER BY ${sort} limit ?,?`;
					connection.query(
						queryStatement,
						[payload.user_id, serialNumber, front, size], // limit 5,1 從第5條開始返回1條紀錄
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'Failed to get the data',
									error: err,
								});
								console.log('Failed to add new meter log!');
							} else {
								res.send({ code: 200, quantity: result.length, data: result });
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*
    get Interval meter log
*/
app.get('/api/chy/device/log/interval', function (req, res) {
	let { serialNumber, start, end } = req.query; // 取得req.body資料

	start = toUtcIso(start);
	end = toUtcIso(end);
	console.log(start);
	console.log(end);
	console.log(serialNumber);

	console.log('get Interval query from device_log ');

	let offset = getLocalTimezoneOffset();

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功

			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement，user_id從JWT拿
					//const queryStatement = `SELECT unit, serialNumber, UpdateTime, T1minusT2, T1, T2 FROM device_log WHERE (user_id = ? AND serialNumber = ?) WHERE UpdateTime BETWEEN ? AND '?;`

					const queryStatement = `
						SELECT 
						unit,
						serialNumber,
						UpdateTime,
						T1minusT2,
						T1,
						T2
						FROM device_log
						WHERE user_id = ?
						AND serialNumber = ?
						AND UpdateTime >= ? 
						AND UpdateTime <= ?;
						`;

					connection.query(
						queryStatement,
						[payload.user_id, serialNumber, start, end], // limit 5,1 從第5條開始返回1條紀錄
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'Failed to get the data',
									error: err,
								});
								console.log('Failed to add new meter log!');
							} else {
								res.send({ code: 200, quantity: result.length, data: result });
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

// 轉ISO
function toUtcIso(localDateStr, timezoneOffset = 8) {
	timezoneOffset = getLocalTimezoneOffset(); // 判斷時區
	const date = new Date(localDateStr);
	// 如果字串沒有時區信息，假設為本地時間
	const offsetMs = timezoneOffset * 60 * 60 * 1000;
	//const utcDate = new Date(date.getTime() - offsetMs);
	const utcDate = new Date(date.getTime());
	return utcDate.toISOString().replace(/\.\d{3}Z$/, 'Z');
}

function getLocalTimezoneOffset() {
	const date = new Date();
	const offsetMinutes = date.getTimezoneOffset();
	const offsetHours = -offsetMinutes / 60; // 注意要取負值

	console.log('Timezone offset (hours):', offsetHours);

	return offsetHours;
}

/*
    用sql語法直接從資料庫取得資料
*/
app.post('/api/chy/device/log/AI', function (req, res) {

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		// JWT驗證
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功

			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// payload.user_id 代替 username
					// queryStatement，user_id從JWT拿
					const queryStatement = req.body.sqlString.replace(
						/user_id\s*=\s*(['"])[^'"]*\1/g,
						`user_id = "${payload.user_id}"`
					);

					connection.query(queryStatement, (err, result) => {
						if (err) {
							res.send({
								code: 200,
								message: 'Failed to get AI search!',
								error: err,
							});
							console.log('Failed to get AI search!');
						} else {
							res.send({
								code: 200,
								message: 'success!',
								data: result,
							});
						}
						connection.release(); // 釋放連線
					});
				}
			});
		}
	});
});

/*
    Upload the file to the server ， 檔案上傳後會暫存到./tmp_uploads資料夾，再搬到./uploads/[username]/[extName]/
*/
app.post('/api/chy/device/file', function (req, res) {
	console.log("Upload user's file!");

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功

			//上傳檔案保留副檔名、設定最大檔案數、設定檔案上傳後，檔案暫存的資料夾，之後會從這個資料夾把檔案寫入目標的資料夾
			const form = new formidable.IncomingForm({
				keepExtensions: true,
				maxFieldsSize: 20 * 1024 * 1024,
				uploadDir: __dirname + '/tmp_uploads/',
			});
			form.parse(req, (err, fields, files) => {
				//上傳檔案，到./upload/username/extName(檔名) 資料夾
				if (err) {
					res.send({ code: 200, message: 'Cannot upload the file', err: err });
				} else if (Object.keys(files).length == 0) {
					res.send({ code: 200, message: 'Cannot be an empty file' });
				} else {
					// console.log(`files: ${files.file[1]}, fields: ${fields.fileName[1]}`);
					const oldPath = files.file[0].filepath; // 檔案暫存的資料夾在 uploadDir:__dirname +"/tmp_uploads/"
					const extName = path.parse(files.file[0].originalFilename).ext.replace('.', ''); // 取得副檔名，例如: jpg不是.jpg，副檔名是來自原檔案，不是使用者額外打的fileName
					
					const rawData = fs.readFileSync(oldPath); // 讀取路徑下的資料
					let newPath = './uploads/' + payload.username + '/' + extName + '/'; // 新檔案要寫入的入徑，__dirname指向目前的路徑
					let newFilename = fields.fileName[0]; // 純檔案名稱，沒有副檔名，例如: newPicture
					let lastDotIndex = newFilename.lastIndexOf('.');
					if (lastDotIndex !== -1) {
						newFilename = newFilename.substring(0, lastDotIndex); // 純檔案名稱，沒有副檔名，例如: newPicture
					}
					const UploadTime = new Date().toISOString(); // 宣告存檔時間
					var newFileNameTemp = newFilename; // 如果沒有重複的檔名就和原本的檔名一樣，寫入資料庫時用newFileNameTemp
					let i = 1, outcome;

					outcome = ifObjectItemIsEmpty({ fileName: fields.fileName[0] }); // 判斷檔名前後有沒空格 or 空字串
					if (outcome.hasErr) {
						return res.send({ code: 200, message: outcome.message });
					}

					// 規範副檔名
					if (
						!(
							extName == 'jpg' ||
							extName == 'jpeg' ||
							extName == 'png' ||
							extName == 'gif' ||
							extName == 'heic' ||
							extName == 'pdf'
						)
					) {
						return res.send({ code: '200', message: 'Must be in image format' });
					}

					// 檢查檔案有沒有存在，如果檔案存在，變更檔名
					while (fs.existsSync(newPath + newFileNameTemp + '.' + extName)) {
						newFileNameTemp = newFilename + '-' + i;
						console.log(newFileNameTemp);
						i++;
					}

					// 檔案路徑加上檔案名稱，之前分開是為了要處理上面while的檔名重複問題，這個就是fs要寫入的檔案路徑
					newPath = newPath + newFileNameTemp + '.' + extName;
					
					// 如果沒有目錄，建立目錄，__dirname/uploads/username/extName(檔名)，檔案存放分使用者，分檔案類型
					if (!fs.existsSync('./uploads/' + payload.username + '/' + extName + '/')) {
						// fs.mkdirSync 要用使用同步的mkdir，因為第一次創建資料夾，還沒創好就繼續執行下面程式的寫入檔案，導致檔案寫入失敗，第二次上傳才成功
						fs.mkdirSync(
							'./uploads/' + payload.username + '/' + extName + '/',
							{ recursive: true },
							function (err) {
								if (err) {
									console.error(err);
									res.send({
										code: 200,
										message: 'Failed to upload data!', 
										error: err,
									});
								}
								console.log('Building directory Success');
							}
						);
					}
					// console.log(newPath);
					fs.writeFile(newPath, rawData, function (err) {
						// 對newPath寫入檔案
						if (err)
							res.send({
								code: 200,
								message: 'Failed to save the file',
								err: err,
							});
						// 寫入檔案失敗
						else {
							// 資料庫連線
							connectionPool.getConnection((connectionError, connection) => {
								if (connectionError) {
									res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
								} else {
									// queryStatement，user_id從JWT拿
									const queryStatement =
										'INSERT INTO data_upload (fileName, path, fileSize, user_id, UploadTime) VALUES (?,?,?,?,?)';
									connection.query(
										queryStatement,
										[
											newFileNameTemp + '.' + extName,
											newPath,
											files.file[0].size,
											payload.user_id,
											UploadTime,
										],
										(err, result) => {
											if (err) {
												res.send({
													code: 200,
													message: 'Failed to upload data',
													error: err,
												});
												console.log('Failed to upload data!');
											} else {
												res.send({ code: 200, files: files });
												console.log('Upload data success');
											}
											connection.release(); // 釋放連線
										}
									);
								}
							});
						}
					});
				}
			});
		}
	});
});

/*  
    Download file from server by filename and user_id in JWT
*/
app.get('/api/chy/device/file/', function (req, res) {
	// let fileName = "newPicture.jpg"; // 取得req.body資料
	// let fileName_utf8 = urlencode.encode(fileName,"utf-8"); //轉成utf8格式，這樣下載圖片的話才不會亂碼，如果不加這行中文檔名得圖片會亂碼

	// fs.readFile('./uploads/theUser/jpg/newPicture.jpg', function(err, data){  // 讀取的檔案路徑為result[0].path
	//   if(err){ // 找不到路徑下的檔案
	//       res.end("file not exist");
	//   }else{ // 找到路徑下的到檔案
	//       res.setHeader("content-disposition","attachment; filename*=utf-8''"+fileName_utf8);
	//       res.contentType("image/png");
	//       res.end(data);
	//   }
	// })

	let { fileName } = req.query; // 取得req.body資料
	let fileName_utf8 = urlencode.encode(fileName, 'utf-8'); //轉成utf8格式，這樣下載圖片的話才不會亂碼，如果不加這行中文檔名得圖片會亂碼

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		// JWT驗證
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功
			user_name = payload.username;

			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement，user_id從JWT拿
					const queryStatement =
						'SELECT path FROM data_upload WHERE user_id = ? and fileName = ?';
					connection.query(queryStatement, [payload.user_id, fileName], (err, result) => {
						if (err) res.send({ code: 200, message: err }); // sql語法有誤
						else if (result.length == 0) {
							// query結果沒有找到，符合user_id = ? and fileName = ?的資料
							res.send({
								code: 200,
								message:
									"The database doesn't have this file's information for this user",
							});
							console.log('Failed to get data!');
						} else {
							fs.readFile(result[0].path, function (err, data) {
								// 讀取的檔案路徑為result[0].path
								if (err) {
									// 找不到路徑下的檔案
									res.end('file not exist');
								} else {
									// 找到路徑下的到檔案
									res.setHeader(
										'content-disposition',
										"attachment; filename*=utf-8''" + fileName_utf8
									); // 設定標頭為檔案下載，以及編碼方式

									isPdf(fileName_utf8)
										? res.contentType('application/pdf')
										: res.contentType('image/png');
									//res.contentType("image/png"); // 設定contentType為圖片
									res.end(data);
								}
							});
						}
						connection.release(); // 釋放連線
					});
				}
			});
		}
	});
});

function isPdf(filename) {
	const ext = path.extname(filename).toLowerCase(); // 統一小寫
	return ext === '.pdf';
}

/*
     Delete data from server by file name and the userid in JWT.
*/
// 先移除檔案再移除資料庫資料
// 1. 檔案移除成功: 1.1 資料庫移除成功 / 1.2 資料庫連線有問題，或其它原因刪除失敗
// 2. 檔案移除失敗: 2.1 檔案刪除失敗就不會進到資料庫刪檔案
app.delete('/api/chy/device/file', function (req, res) {
	console.log("delete user's file!");

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功
			// 取得檔案名稱及副檔名
			// 如果是單個物件有 bug
			filesname = req.query.fileName;

			// 這是個 filename 陣列
			// 1.一個物件 (dog.jpg)             => filesname[0] = "d"
			// 2.二個以上: (dog.jpg、bird.jpg)  => filesname[0] = "dog.jpg"
			// 判斷方法(不確定有沒有 b u g)      => 檢查 filesname[0].length > 1 && filesname[0].includes(".")

			// 琢磨一下看看，透過 template_id和 user_id去抓路徑
			if (filesname[0].length == 1) {
				filesname = [filesname];
			}

			let ext = new Array(req.query.fileName.length);
			let deleteFilePath = new Array(req.query.fileName.length);

			for (i = 0; i < filesname.length; i++) {
				ext[i] = path.parse(filesname[i]).ext.replace('.', ''); // 副檔名
				deleteFilePath[i] =
					'./uploads/' + payload.username + '/' + ext[i] + '/' + filesname[i]; // 刪除檔案的路徑
			}

			if (!fs.existsSync(deleteFilePath[0])) {
				res.send({ code: 200, message: 'The file to be deleted does not exist!' });
			} else {
				deleteFilePath.forEach((file) => {
					try {
						fs.unlinkSync(file);
						console.log(`Deleted file: ${file}`);
					} catch (err) {
						console.error(`Error deleting file: ${file}`, err);
						return res.send({
							code: 200,
							message: `Error deleting file: ${file}`,
							err,
						});
					}
				});

				connectionPool.getConnection((connectionError, connection) => {
					// 資料庫連線
					if (connectionError) {
						res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
					} else {
						// queryStatement，user_id從JWT拿
						const queryStatement =
							'DELETE FROM data_upload WHERE user_id = ? and path IN (?)';
						connection.query(
							queryStatement,
							[payload.user_id, deleteFilePath],
							(err, result) => {
								if (err) {
									res.send({
										code: 200,
										message: 'Failed to delete data!',
										error: err,
									});
									console.log('Failed to delete data!');
								} else {
									res.send({ code: 200, message: 'Delete data success!' });
									console.log('Delete data success!');
								}
								connection.release(); // 釋放連線
							}
						);
					}
				});
			}
		}
	});
});

/*
      show file from server
 */
app.get('/api/chy/device/file/show', function (req, res) {
	let { fileName } = req.query; // 取得req.body資料

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		// JWT驗證
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功
			user_name = payload.username;

			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement，user_id從JWT拿
					const queryStatement =
						'SELECT path FROM data_upload WHERE user_id = ? and fileName = ?';
					connection.query(queryStatement, [payload.user_id, fileName], (err, result) => {
						if (err) res.send({ code: 200, message: err }); // sql語法有誤
						else if (result.length == 0) {
							// query結果沒有找到，符合user_id = ? and fileName = ?的資料
							res.send({
								code: 200,
								message: "The database doesn't have this file's information.",
							});
							console.log('Failed to get data!');
						} else {
							fs.readFile(result[0].path, function (err, data) {
								// 讀取的檔案路徑為result[0].path
								if (err) {
									// 找不到路徑下的檔案
									res.end('file not exist');
								} else {
									// 找到路徑下的到檔案
									res.writeHead(200, { 'Content-Type': 'image/png' }); // write header
									res.end(data); // Send the file data to the browser
								}
							});
						}
						connection.release(); // 釋放連線
					});
				}
			});
		}
	});
});

/*
    List all uploaded files of the user by user_id in JWT
*/
app.get('/api/chy/device/file/list', function (req, res) {

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		// JWT驗證
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功

			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement，user_id從JWT拿
					const queryStatement =
						"SELECT fileName, path, fileSize, UploadTime FROM data_upload WHERE user_id = ? and path LIKE './uploads%' ORDER BY UploadTime DESC";
					connection.query(queryStatement, [payload.user_id], (err, result) => {
						let totalFileSize = 0; // 計算總檔案大小
						for (let i = 0; i <= Object.keys(result).length - 1; i++) {
							// 累加檔案大小
							totalFileSize += result[i].fileSize;
						}

						let totalFileSizes = changeFileSizeFormat(totalFileSize); // 轉換總檔案大小的單位

						if (err) {
							res.send({
								code: 200,
								message: 'Failed to get data list!',
								error: err,
							});
							console.log('Failed to get data list!');
						} else {
							res.send({
								code: 200,
								message: 'success!',
								number_of_files: Object.keys(result).length,
								total_fileSize: totalFileSizes,
								data: result,
							});
						}
						connection.release(); // 釋放連線
					});
				}
			});
		}
	});
});

/*
	 保存 template 裡的 HTML , saveTemplateHtml feedback
*/
app.post('/api/chy/file/pdfTemplate', function (req, res) {
	console.log('save pdf template html');

	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	const syncTime = Date.now();
	let { template_id, template_name, paper_size, template_html, UploadTime } = req.body;

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection((connectionError, connection) => {
				if (connectionError) {
					res.send({ code: 200, message: connectionError });
				} else {
					const queryStatement =
						'INSERT INTO pdf_template_html (user_id, template_id, template_name, paper_size, template_html, UploadTime, invalid, isDeleted, isSynced, syncTime) VALUES (?,?,?,?,?,?,?,?,?,?)';
					connection.query(
						queryStatement,
						[
							payload.user_id,
							template_id,
							template_name,
							paper_size,
							template_html,
							UploadTime,
							0,
							0,
							0,
							syncTime,
						],
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'error',
									error: err,
								});
							} else {
								res.send({
									code: 200,
									message: 'Success',
									syncTime: syncTime,
								});
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*
	獲得 template HTML
*/
app.get('/api/chy/file/pdfTemplate', function (req, res) {
	let { template_id } = req.query;
	
	// 驗證 headers 有沒有帶 jwt token，有的話分割 bearer 字串和 token 的 value
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
	
	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection((connectionError, connection) => {
				if (connectionError) {
					res.send({ code: 200, message: connectionError });
				} else {
					const queryStatement = `
							SELECT
								MAX(h.template_html) AS template_html,
								MAX(h.paper_size) AS paper_size,
								GROUP_CONCAT(i.path ORDER BY i.file_id SEPARATOR ',') AS image_paths
							FROM pdf_template_html h
							LEFT JOIN pdf_template_img i 
								ON h.user_id = i.user_id 
								AND h.template_id = i.template_id
							WHERE h.user_id = ?
								AND h.template_id = ?
							GROUP BY h.template_id, h.user_id
							`;

					connection.query(
						queryStatement,
						[payload.user_id, template_id],
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: err,
								});
								return;
							} else if (Object.keys(result).length === 0) {
								res.send({ code: 200, message: 'no data' });
							} else {
								const data = result[0];
								const imagePaths = data.image_paths
									? data.image_paths.split(',')
									: []; // 將字串轉成陣列
								res.status(200).json({
									code: 200,
									message: 'Success',
									paper_size: data.paper_size,
									html: data.template_html, // 原始字串（含換行）
									imagePaths: imagePaths,
								});
							}
						}
					);
				}
			});
		}
	});
});

/*
    delete template by template_id
*/
app.post('/api/chy/file/pdfTemplate/del', function (req, res) {
	console.log('template del query: ' + req.query.template_id);
	let { template_id } = req.body; 

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection((connectionError, connection) => {
				if (connectionError) {
					// 資料庫連線
					res.send({ code: 200, message: connectionError });
				} else {
					const isDeleted = Date.now();
					const queryStatementHTML = 
						'UPDATE pdf_template_html SET isDeleted = ? WHERE user_id = ? and template_id = ?';;
					connection.query(
						queryStatementHTML,
						[isDeleted, payload.user_id, template_id], 
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'template_id does not exist!',
									error: err,
								});
								return;
							}
						}
					);

					const queryStatementIMG =
						'DELETE FROM pdf_template_img WHERE user_id = ? and template_id =?';
					connection.query(
						queryStatementIMG,
						[payload.user_id, template_id],
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'template_id does not exist!',
									error: err,
								});
								return;
							}
							connection.release();
						}
					);

					// 刪實體檔案: 刪 整個 template_id 資料夾 (從 JS傳進來的檔案，都會 轉換 成 PNG)
					const deleteFolder =
						'./uploads/' + payload.username + '/pdfTemplate/' + template_id;

					// 1.解析成絕對路徑
					const absPath = path.resolve(deleteFolder);
					console.log(absPath);

					// 2. 不允許刪除跟目錄 & 空路徑
					if (!absPath || absPath === '/' || absPath === path.parse(absPath).root) {
						res.send({ code: 200, message: 'Refuse to delete invalid or root path' });
						return;
					} else if (fs.existsSync(absPath)) {
						// 3. 刪除路徑下的檔案
						fs.rmSync(absPath, { recursive: true, force: true });
					}

					res.send({
						code: 200,
						message: 'template_id ' + template_id + ' delete successful!',
					});
				}
			});
		}
	});
});

/*
	get template list 
*/
app.get('/api/chy/file/pdfTemplate/list', function (req, res) {

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection((connectionError, connection) => {
				if (connectionError) {
					res.send({ code: 200, message: connectionError });
				} else {
					const queryStatement = `SELECT id, template_id, template_name, paper_size, UploadTime FROM pdf_template_html WHERE user_id = ? and isDeleted = 0 ORDER BY "id"`;

					connection.query(queryStatement, [payload.user_id], (err, result) => {
						if (err) {
							res.send({
								code: 200,
								message: err,
							});
						} else if (Object.keys(result).length === 0) {
							res.send({ code: 200, message: 'no data' });
						} else {
							res.status(200).json({
								code: 200,
								message: 'Success',
								data: result,
							});
						}
					});
				}
			});
		}
	});
});

/*
    update pdf template_name
*/
app.put('/api/chy/file/pdfTemplate/list', function (req, res) {
	const { template_id, template_name, isSynced } = req.body;
	let outcome;

	// 判斷 req.body 裡的 欄位數值 是不是空字串
	outcome = ifObjectItemIsEmpty(req.body);
	if (outcome.hasErr) {
		return res.send({ code: 200, message: outcome.message });
	}

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection((connectionError, connection) => {
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					const queryStatement =
						'UPDATE pdf_template_html SET template_name = ?, isSynced = ?  WHERE user_id = ? and template_id = ?';
					connection.query(
						queryStatement,
						[template_name, isSynced, payload.user_id, template_id],
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'template name does not exist!',
									error: err,
								});
							} else {
								res.send({
									code: 200,
									message: 'updated successfully!',
								});
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*
	保存 template 裡的 picture
*/
app.post('/api/chy/file/pdfTemplate/image', function (req, res) {
	console.log('Upload pdf template image file!');
	
	let taskId = "";
	const token = typeof req.headers.authorization === 'string'
					? req.headers.authorization.split(' ')[1]
					: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => { 
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			//上傳檔案保留副檔名、設定最大檔案數、設定檔案上傳後，檔案暫存的資料夾，之後會從這個資料夾把檔案寫入目標的資料夾
			const form = new formidable.IncomingForm({
				keepExtensions: true,
				maxFieldsSize: 20 * 1024 * 1024,
				uploadDir: __dirname + '/tmp_uploads/',
			});
			form.parse(req, (err, fields, files) => { 

				if (err) {
					res.send({ code: 200, message: 'Cannot upload the file', err: err });
				} else if (Object.keys(files).length == 0) {
					res.send({ code: 200, message: 'Cannot be an empty file' });
				} else {
					const UploadTime = fields.UploadTime[0]; 
					const invalid = fields.invalid[0];
					const isDeleted = fields.invalid[0];
					const isSynced = fields.isSynced[0]; 
					const syncTime = fields.syncTime[0];
					taskId = fields.taskId[0]; 

					// console.log(`files: ${files.file[1]}, fields: ${fields.fileName[1]}`);
					const oldPath = files.file[0].filepath; // 檔案暫存的資料夾在 uploadDir:__dirname +"/tmp_uploads/"
					const extName = path.parse(files.file[0].originalFilename).ext.replace('.', ''); // 取得副檔名，例如: jpg不是.jpg，副檔名是來自原檔案，不是使用者額外打的fileName

					const rawData = fs.readFileSync(oldPath);
					let newPath =
						'./uploads/' +
						payload.username +
						'/pdfTemplate/' +
						fields.template_id[0] +
						'/' +
						extName +
						'/'; // 新檔案要寫入的入徑，__dirname指向目前的路徑
					let newFilename = fields.fileName[0]; // 純檔案名稱，沒有副檔名，例如: newPicture
					let lastDotIndex = newFilename.lastIndexOf('.');
					if (lastDotIndex !== -1) {
						newFilename = newFilename.substring(0, lastDotIndex); // 純檔案名稱，沒有副檔名，例如: newPicture
					}
					var newFileNameTemp = newFilename; // 如果沒有重複的檔名就和原本的檔名一樣，寫入資料庫時用newFileNameTemp
					let i = 1, outcome;

					outcome = ifObjectItemIsEmpty({ fileName: fields.fileName[0] }); // 判斷檔名前後有沒空格 or 空字串
					if (outcome.hasErr) {
						return res.send({ code: 200, message: outcome.message });
					}

					// 規範副檔名
					if (
						!(
							extName == 'jpg' ||
							extName == 'jpeg' ||
							extName == 'png' ||
							extName == 'gif' ||
							extName == 'heic' ||
							extName == 'pdf'
						)
					) {
						return res.send({ code: '200', message: 'Must be in image format' });
					}

					// 檔案路徑加上檔案名稱，之前分開是為了要處理上面while的檔名重複問題，這個就是fs要寫入的檔案路徑
					newPath = newPath + newFileNameTemp + '.' + extName;

					// 如果沒有目錄，建立目錄，__dirname/uploads/username/extName(檔名)，檔案存放分使用者，分檔案類型
					if (
						!fs.existsSync(
							'./uploads/' +
								payload.username +
								'/pdfTemplate/' +
								fields.template_id[0] +
								'/' +
								extName +
								'/'
						)
					) {
						// fs.mkdirSync 要用使用同步的mkdir，因為第一次創建資料夾，還沒創好就繼續執行下面程式的寫入檔案，導致檔案寫入失敗，第二次上傳才成功
						fs.mkdirSync(
							'./uploads/' +
								payload.username +
								'/pdfTemplate/' +
								fields.template_id[0] +
								'/' +
								extName +
								'/',
							{ recursive: true },
							function (err) {
								if (err) {
									console.error(err);
									res.send({
										code: 200,
										message: 'Failed to upload data!',
										error: err,
									});
								}
								console.log('Building directory Success');
							}
						);
					}

					fs.writeFile(newPath, rawData, function (err) {
						// 對newPath寫入檔案
						if (err)
							res.send({
								code: 200,
								message: 'Failed to save the file',
								err: err,
							});
						else {
							connectionPool.getConnection((connectionError, connection) => {
								if (connectionError) {
									res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
								} else {
									const queryStatement =
										'INSERT INTO pdf_template_img (template_id, fileName, path, fileSize, user_id, UploadTime, invalid, isDeleted, isSynced, syncTime) VALUES (?,?,?,?,?,?,?,?,?,?)';
									connection.query(
										queryStatement,
										[
											fields.template_id[0],
											newFileNameTemp + '.' + extName,
											newPath,
											files.file[0].size,
											payload.user_id,
											UploadTime,
											invalid,
											isDeleted,
											isSynced,
											syncTime,
										],
										(err, result) => {
											if (err) {
												res.send({
													code: 200,
													message: 'Failed to upload data',
													error: err,
												});
												console.log('Failed to upload data!');
											} else {
												res.send({ code: 200, files: files });
												console.log('Upload data success');
											}
											connection.release(); // 釋放連線
										}
									);
								}
							});
						}
					});
				}
			});
		}

		// 更新 timeout
		refreshTimer(taskId, payload.user_id);
	});
});

/*
      獲得 template 裡的圖片 
*/
app.get('/api/chy/file/pdfTemplate/image', function (req, res) {
	let { path } = req.query; // 取得req.body資料

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		// JWT驗證
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功
			fs.readFile(path, function (err, data) {
				if (err) {
					// 找不到路徑下的檔案
					res.end('file not exist');
				} else {
					// 找到路徑下的到檔案
					res.writeHead(200, { 'Content-Type': 'image/png' }); // write header
					res.end(data); // Send the file data to the browser
				}
			});
		}
	});
});


/*
      獲得 template 裡的圖片 
*/
app.post('/api/chy/file/pdfTemplate/image/get', function (req, res) {
	let { path } = req.body; // 取得req.body資料

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		// JWT驗證
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功
			fs.readFile(path, function (err, data) {
				if (err) {
					// 找不到路徑下的檔案
					res.end('file not exist');
				} else {
					// 找到路徑下的到檔案
					res.writeHead(200, { 'Content-Type': 'image/png' }); // write header
					res.end(data); 
				}
			});
		}
	});
});

/*  
    upload local unSync template_HTML to cloud
*/

app.post('/api/chy/file/pdfTemplate/uploadHtml2cloud', function (req, res) {
	console.log('upload local unSync template_HTML to cloud');

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// const syncTime = Date.now(); // 是 UTC 時間不是local time
			let data = [];
			for (let i = 0; i < req.body.length; i += 1) { 
				data.push(Object.values(req.body[i]).slice(0, -1).concat([payload.user_id]));
			}
			
			connectionPool.getConnection(async (connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {

					// await
					try {
						// 1.插入資料
						const queryStatement =
						'INSERT INTO pdf_template_html (template_id, template_name, paper_size, template_html, UploadTime, invalid, isDeleted, isSynced, syncTime, user_id) VALUES ?';
						const [results] = await connection.promise().query(queryStatement, [data]);

						// 2. 撈出 max id
						const last= data[data?.length-1];  
						const [rows] = await connection.promise().query('SELECT id FROM pdf_template_html WHERE user_id = ? and template_id = ?  LIMIT 1', [payload.user_id, last[0]]); 
						
						res.send({ code: 200, message: 'Success', lastId: rows?.[0]?.id ?? 0}); 
					} catch(ex) {
						res.send({code: 200, message: 'err', error: ex.message});
							console.log('Failed to upload local unSync template_HTML to cloud!');

					} finally {
						connection.release(); // 釋放連線 Failed 
					}
				}
			});
		}

		// 更新 timeout
		var taskId = req.body[0].taskId; 
		const tmpIds = req.body.map(item => Object.values(item).at(0)); 
		
		onDataReceived(taskId, payload.user_id, tmpIds, 0, false); 
	});
});

/*
	Check pdf template sync status 
*/
app.post('/api/chy/file/pdfTemplate/ifUploadSync', function (req, res) {
	console.log('Check if pdfTemplate sync');

	let { syncTime } = req.body;

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null;  

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError });
				} else {
					// 這裡的 syncTime 指的是 local DB 最後同步時間
					const queryStatement = `SELECT id FROM pdf_template_html WHERE user_id = ? and syncTime > ? ORDER BY id LIMIT 4`;
					connection.query(queryStatement, [payload.user_id, syncTime], (err, result) => {
						if (err) {
							res.send({
								code: 200,
								message: 'Unable to check synced status',
								error: err,
							});
							console.log('Unable to check synced status');
						} else {
							res.send({
								code: 200, 
								message: 'Success', 
								needSync: result.length > 0 ? true : false, // 這裡是 check 有無資料，所以>0 就同步。
								num: result.length == 4 ? result.length - 1 : result.length,
								hasMoreData: result.length == 4 ? true : false,
								firstId: result.length > 0 ? result[0]?.id : -1, // 第一筆就是 nextId，因為這裡只是check，跟傳資料的循環有點不同
								currentSyncTime: Date.now(), 
							});
						}
						connection.release(); // 釋放連線
					});
				}
			});
		}
	});
});

/*
	Pdf template sync
*/
app.post('/api/chy/file/pdfTemplate/uploadSync', function (req, res) {
	console.log('Data log sync');
	let { nextId, lastSyncTime, currentSyncTime } = req.body;

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection(async (connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError });
				} else {
					try {
						
						// 第一層查詢
						const queryStatementHTML = ` 
							SELECT id, template_id, template_name, paper_size, template_html, UploadTime, invalid, isDeleted, isSynced, syncTime 
							FROM pdf_template_html 
							WHERE user_id = ? and syncTime > ? and syncTime <= ? and id >= ? ORDER BY id LIMIT 4`;
							
						// mysql2 await/promise 的回傳值不是 array，[rows, fields]: [查詢結果, 欄位的 meta data]
						// [] 解構陣列的第一個元素，也就是 row
						const [htmlResults] = await connection 
							.promise()
							.query(queryStatementHTML, [payload.user_id, lastSyncTime, currentSyncTime, nextId]);

						const hasExtra = htmlResults.length === 4; 
						const htmlData = htmlResults;

						if (htmlData.length === 0) {
							res.send({
								code: 200,
								message: 'No data found',
								needSync: false,
								num: 0,
								nextId: -1,
								html: [],
								img: [],
							});
							return;
						}

						// 第二層查詢 - 基於第一層結果
						// const ids = htmlResults.map(row => row.template_id);  // 要比對的 template_id
						const ids = htmlResults.slice(0, 3).map(row => row.template_id); // 查詢至多4個，不可有第4個
						const placeholders = ids.map(() => '?').join(','); 
						
						const queryStatementIMG = ` 
							SELECT template_id, fileName, path, fileSize, UploadTime, invalid, isDeleted, isSynced, syncTime 
							FROM pdf_template_img 
							WHERE user_id = ? and syncTime > ? and syncTime <= ? and template_id IN (${placeholders}) and isDeleted = 0`;
						
						const [imgResults] = await connection
							.promise()
							.query(queryStatementIMG, [payload.user_id, lastSyncTime, currentSyncTime, ...ids]); 

						res.send({
							code: 200,
							message: 'Successfully check synced status!', 
							needSync: hasExtra,
							num: hasExtra ? 3 : htmlData.length,
							nextId: hasExtra ? htmlData[3]?.id : -1,
							html: hasExtra ? htmlData.slice(0, 3) : htmlData, // slice(0, 3) 是 0、1、2
							img: imgResults,
							lastId: htmlData?.[hasExtra ? 2 : htmlData.length - 1]?.id ?? 0
						});
						
					} catch (err) {
						// Unable to Check synced
						console.error('Unable to check synced status:', err); 
						res.send({
							code: 200,
							message: 'Unable to check synced status',
							error: err.message,
						});
					} finally {
						connection.release(); 
					}
				}
			});
		}
	});
});

/*
	Check if delete template sync status 
*/
app.post('/api/chy/device/template/checkIfPdfTemplateDel', function (req, res) {
	console.log('Check if template delete sync');

	let { isDeleted, currentSyncTime } = req.body; 

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError });
				} else {
					// 查詢 1：取得最大 syncTime
					
					const queryStatement = `SELECT id FROM pdf_template_html WHERE user_id = ? and isDeleted > ? and syncTime <= ? ORDER BY id LIMIT 21`;
					connection.query(
						queryStatement,
						[payload.user_id, isDeleted, currentSyncTime], 
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'Unable to check synced status',
									error: err,
								});
								console.log('Unable to check synced status');
							} else {
								res.send({
									code: 200,
									message: 'Successfully check synced status!',
									needSync: result.length > 0 ? true : false,
									num: result.length == 21 ? result.length - 1 : result.length,
									hasMoreData: result.length == 21 ? true : false,
									firstId: result[0]?.id ?? -1, 
								});
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*
	sync delete template
*/
app.post('/api/chy/device/template/syncPdfTemplateDel', function (req, res) {
	console.log('Delete template sync');

	let { nextId, isDeleted, currentSyncTime, taskId } = req.body;

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection(async (connectionError, connection) => {

				if (connectionError) {
					res.send({ code: 200, message: connectionError }); 
				} else {
					const queryStatement = ` 
							SELECT id, template_id, template_name, isDeleted 
						 	FROM pdf_template_html 
						 	WHERE user_id = ? and isDeleted > ? and id >= ? and syncTime <= ?
						 	ORDER BY id LIMIT 21
						`; 
					connection.query(
						queryStatement,
						[payload.user_id, isDeleted, nextId, currentSyncTime], 
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'Unable to check synced status',
									error: err,
								});
								console.log('Unable to check synced status');
							} else {
								const hasExtra = result.length == 21;

								res.send({
									code: 200,
									message: 'Success!',
									needSync: hasExtra ? true : false,
									num: hasExtra ? 20 : result.length,
									nextId: hasExtra ? result[20].id : -1, 
									lastId: result?.[hasExtra ? 19  : result.length - 1]?.id ?? 0,
									data: hasExtra ? result.slice(0, 20) : result, // 1144
								});
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}

		refreshTimer(taskId, payload.user_id);
	});
});


/*
	Update isDeleted template from local pc
*/
app.post('/api/chy/device/template/pdfTemplateDel', function (req, res) {
	console.log('update delete template');

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// const isDeleted = Date.now(); 
			let data = [];
			for (let i = 0; i < req.body.length; i += 1) 
				data.push(Object.values(req.body[i]).slice(0, -1).concat([payload.user_id])); 
			
			connectionPool.getConnection(async (connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					const values = data
						.map((item) => `('${item[0]}', ${item[1]}, ${item[2]})`)
						.join(',\n');

					// 分成 6 步執行
					try {
						// 1. 建立臨時表
						await connection.promise().query(` 
							CREATE TEMPORARY TABLE temp_sync_updates ( 
								template_id VARCHAR(32),
								isDeleted BIGINT,
								user_id INT,
								PRIMARY KEY (template_id, isDeleted, user_id) 
							)
						`);

						// 2. 插入資料
						await connection.promise().query(`
							INSERT INTO temp_sync_updates VALUES ${values}
						`);

						// 3. 更新 pdf_template_html
						await connection.promise().query(`
							UPDATE pdf_template_html t
							INNER JOIN temp_sync_updates u 
								ON t.template_id = u.template_id 
								AND t.user_id = u.user_id
							SET t.isDeleted = u.isDeleted
						`);

						// 4. 更新 pdf_template_img
						await connection.promise().query(`
							UPDATE pdf_template_img t
							INNER JOIN temp_sync_updates u 
								ON t.template_id = u.template_id 
								AND t.user_id = u.user_id
							SET t.isDeleted = u.isDeleted
						`);

						const templateIds = []; 
						for (const item of req.body)
							 templateIds.push(item.template_id); 

						// 4.1 刪除pdf_template_img 
						await connection.promise().query(` 
							DELETE FROM pdf_template_img 
							WHERE template_id in (?) and user_id = ? 
						`, [templateIds, payload.user_id]); 

						// 5. 清理（可選，連線關閉時會自動清理）
						await connection.promise().query(`DROP TEMPORARY TABLE temp_sync_updates`);
						
						// 6.刪除圖片 根據 template_id 和 user_name 
						for (const item of req.body) { 
							let deleteFolder =  `./uploads/${payload.username}/pdfTemplate/${item.template_id}`;

							// 1.解析成絕對路徑 
							const absPath = path.resolve(deleteFolder);

							// 2. 不允許刪除跟目錄 & 空路徑
							if (!absPath || absPath === '/' || absPath === path.parse(absPath).root) {
								res.send({ code: 200, message: 'Refuse to delete invalid or root path' });
								return;
							} else if (fs.existsSync(absPath)) { 
								// 3. 刪除路徑下的檔案
								fs.rmSync(absPath, { recursive: true, force: true });
							}
						};

						// 取得最後一筆的 id
						var last = data[data.length-1]; 
						const [rows] = await connection.promise().query('SELECT id FROM pdf_template_html WHERE template_id =? and isDeleted = ? and user_id = ? LIMIT 1', [last[0], last[1], last[2]]);
						res.send({code: 200, message: 'Success', lastId: rows?.[0]?.id ?? 0});
					
					} catch (error) {
						res.send({code: 200, message: 'Unable to check synced status', error: err});
						console.error('更新失敗:', error);
					}
				}
			});
		}

		// 更新 timeout
		var taskId = req.body[0].taskId; 
		refreshTimer(taskId, payload.user_id);
	});
});

/*
	Pdf template sync
*/ 
app.post('/api/chy/device/template/checkLastPdfSyncTime', function (req, res) {
	console.log('Data log sync');
	let { lastSyncTimeId, lastSyncTime } = req.body;

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection(async (connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError });
				} else {
					try {
						
						// 第一層查詢 這裡要等於才行不然少一筆
						const queryStatementHTML = ` 
							SELECT id, template_id, template_name, paper_size, template_html, UploadTime, invalid, isDeleted, isSynced, syncTime 
							FROM pdf_template_html 
							WHERE user_id = ? and syncTime <= ? and id >= ? ORDER BY id LIMIT 4`; 
							
						// mysql2 await/promise 的回傳值不是 array，[rows, fields]: [查詢結果, 欄位的 meta data]
						// [] 解構陣列的第一個元素，也就是 row
						const [htmlResults] = await connection 
							.promise()
							.query(queryStatementHTML, [payload.user_id, lastSyncTime, lastSyncTimeId]);

						const hasExtra = htmlResults.length === 4; 
						const htmlData = htmlResults;

						if (htmlData.length === 0) {
							res.send({code: 200, message: 'No data found', needSync: false, num: 0, nextId: null, html: [], img: []});
							return; 
						}

						// 第二層查詢 - 基於第一層結果
						//const ids = htmlResults.map(row => row.template_id);  // 要比對的 template_id
						const ids = htmlResults.slice(0, 3).map(row => row.template_id);
						const placeholders = ids.map(() => '?').join(','); 
						// placeholders = ids.map(() => '?').join 
						const queryStatementIMG = ` 
							SELECT template_id, fileName, path, fileSize, UploadTime, invalid, isDeleted, isSynced, syncTime 
							FROM pdf_template_img 
							WHERE user_id = ? and syncTime <= ? and template_id IN (${placeholders}) and isDeleted = 0`;
						
						const [imgResults] = await connection 
							.promise()
							.query(queryStatementIMG, [payload.user_id, lastSyncTime, ...ids]); 
							

						res.send({
							code: 200,
							message: 'Successfully check synced status!', 
							needSync: hasExtra, // 超過就要同步
							num: hasExtra ? 3 : htmlData.length,
							nextId: hasExtra ? htmlData[3]?.id : -1,
							html: hasExtra ? htmlData.slice(0, 3) : htmlData, // slice(0, 3) 是 0、1、2
							img: imgResults,
							lastId: htmlData?.[hasExtra ? 2 : htmlData.length - 1]?.id ?? 0 // 當左側是 null 或 undefined時，使用右側值
						}); 
						
					} catch (err) {
						// Unable to Check synced
						console.error('Unable to check synced status:', err); 
						res.send({
							code: 200,
							message: 'Unable to check synced status',
							error: err.message,
						});
					} finally {
						connection.release(); 
					}
				}
			});
		}
	});
});

/*	
	sync delete template checkLastPdfSyncTime
*/
app.post('/api/chy/device/template/checkLastPdfIsDeleted', function (req, res) {
	console.log('Check Last Pdf isDeleted');

	let { lastIsDeletedId, lastIsDeleted } = req.body;

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection(async (connectionError, connection) => {

				if (connectionError) {
					res.send({ code: 200, message: connectionError }); 
				} else {
					const queryStatement = ` 
							SELECT id, template_id, template_name, isDeleted 
						 	FROM pdf_template_html 
						 	WHERE user_id = ? and isDeleted <= ? and id >= ? 
						 	ORDER BY id LIMIT 11
						`; 
					connection.query(
						queryStatement,
						[payload.user_id, lastIsDeleted, lastIsDeletedId], 
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'Unable to check synced status',
									error: err,
								});
								console.log('Unable to check synced status');
							} else {
								const hasExtra = result.length == 11; 
								res.send({
									code: 200,
									message: 'Success!',
									needSync: hasExtra ? true : false,
									num: hasExtra ? 10 : result.length,
									nextId: hasExtra ? result[10].id : -1, 
									lastId: result?.[hasExtra ? 9  : result.length - 1]?.id ?? 0,
									data: hasExtra ? result.slice(0, 10) : result, 
								});
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*	
	sync Template Rename 
*/
app.post('/api/chy/device/template/syncTemplateRename', function (req, res) {
	console.log('sync Template Rename'); 

	let data = [];
	for (let i = 0; i < req.body.length; i += 1)
		data.push(Object.values(req.body[i])); 
	 
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
	
	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection(async (connectionError, connection) => { 

				if (connectionError) {
					res.send({ code: 200, message: connectionError }); 
				} else {

					try {
						// 1) 建立臨時表
						await connection
							.promise()
							.query(` 
								CREATE TEMPORARY TABLE temp_sync (
									template_id VARCHAR(32),
									template_name VARCHAR(100),
									new_isSynced BIGINT
								)
							`); 
						
						// 2) 批次插入資料
						// 不會攤平 二維陣列: VALUES (?, ?, ?), (?, ?, ?)
						// 會  攤平 二維振烈: VALUES ?
						const insertValues = data 
							.map(() => '(?, ?, ?)') // ()代表可迭代的其中一項 
							.join(', '); // (?, ?, ?), (?, ?, ?), (?, ?, ?) 用於插入大量資料 

						const insertParams = [];
						// data = [
						// 	['7f40bff1c6c41154', 1769387469936],
						// 	['7f40bff1c6c41155', 1769387469937],
						// ]; 取每條陣列的 0、1 index 命名為 template_id, isSynced
						
						// 如果傳 data 就是 data[0], 傳[data]是，[]裡面的第一個，也就是整個二維陣列的 data
						// `INSERT INTO temp_sync (template_id, new_isSynced) VALUES ?`, [data]

						// `INSERT INTO temp_sync (template_id, new_isSynced) VALUES ${insertValues}`,	insertParams
						// insertParams = [
						// 	'7f40bff1c6c41154', 1769387469936,
						// 	'7f40bff1c6c41155', 1769387469937 
						// ]

						// 2 維轉 1 維列，餵進去 
						data.forEach(([template_id, template_name, isSynced]) => { // template_name, isSynced
							insertParams.push(template_id, template_name, isSynced);
						});

						await connection
							.promise()
							.query(
								`INSERT INTO temp_sync (template_id, template_name, new_isSynced) VALUES ?`,
								[data]
							); 

						// 3) 先取出需要更新的 template_id 
						// 檢查 如果 cloud > local rename time，回傳給 local
						const [updatedRows] = await connection 
							.promise()
							.query(`
								SELECT h.template_id, h.template_name, h.isSynced
								FROM pdf_template_html AS h
								JOIN temp_sync AS t
									ON h.template_id = t.template_id
								WHERE t.new_isSynced < h.isSynced
							`); 

						// 4) 再執行 JOIN 更新 pdf_template_html 
						// 更新 local > cloud rename time
						const [result] = await connection
								.promise() 
								.query(`
									UPDATE pdf_template_html AS h
									JOIN temp_sync AS t
										ON h.template_id = t.template_id
									SET h.isSynced = t.new_isSynced, h.template_name = t.template_name
									WHERE t.new_isSynced > h.isSynced 
								`,); 

						// 5) 清理臨時表
						await connection
							.promise()
							.query('DROP TEMPORARY TABLE IF EXISTS temp_sync'); 
						
						// 6) 回傳結果 
						res.send({
							code: 200,
							message: 'Success',
							data: updatedRows ?? [] 
						});
						
					} catch (error) {
						console.error('批次更新失敗:', error); 
						
						// 清理臨時表
						try {
							await connection.promise().query('DROP TEMPORARY TABLE IF EXISTS temp_sync');
						} catch (cleanupError) { 
							console.error('清理臨時表失敗:', cleanupError); // EXISTS temp_sync
						}
						
						res.status(200).send({
							code: 500,
							message: 'Batch update failed', 
							error: error.message 
						});

					} finally {
						connection.release(); // 釋放連線
					}
					
				}
			});
		}
	});
});



/*
	test
*/
app.post('/api/chy/device/test/log', function (req, res) {
	console.log('Upload data from meter');

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// 取得req.body資料，在API上的格式為[{...}, {...}]，讀進來為物件的陣列，處理並轉換成二維陣列，以利SQL使用，並加上JWT裡的user_id
			// 此外配合後面WHERE判斷，要插入user_log表格的serialNumber和device_id，是不是和user_info裡的的serialNumber和device_id，有沒有完全對應
			// 例如: 不可以出現(device_id, serialNumber)在user_info表格為(0, CHYTM1)，插入user_log表格時為(0, CHYTM2)
			// 所以再加上device_id和serialNumber的restriction
			const syncTime = Date.now();
			let data = [];
			for (let i = 0; i < req.body.length; i += 1) {
				// 堆疊 device_log 檔案
				data.push(Object.values(req.body[i]).concat([syncTime, payload.user_id])); 
			}

			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					const queryStatement =
						'INSERT INTO TH928_log (gid ,mac, T1, T2, unit, type, serialNumber, datetime, uploadtime, invalid, isDeleted, isSynced, syncTime, user_id) VALUES ?';
					connection.query(queryStatement, [data], (err, result) => {
						if (err) {
							res.send({
								code: 200,
								message: 'Failed to add meter log!',
								error: err,
							});
							console.log('Failed to add meter log!');
						} else {
							res.send({ code: 200, message: 'Success', syncTime: syncTime });
						}
						connection.release(); // 釋放連線
					});
				}
			});
		}
	});
});

/*
	upload data log
*/
app.post('/api/chy/device/test/logWithSyncTime', function (req, res) {
	console.log('Upload data from meter with SyncTime');

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// 取得req.body資料，在API上的格式為[{...}, {...}]，讀進來為物件的陣列，處理並轉換成二維陣列，以利SQL使用，並加上JWT裡的user_id
			// 此外配合後面WHERE判斷，要插入user_log表格的serialNumber和device_id，是不是和user_info裡的的serialNumber和device_id，有沒有完全對應
			// 例如: 不可以出現(device_id, serialNumber)在user_info表格為(0, CHYTM1)，插入user_log表格時為(0, CHYTM2)
			// 所以再加上device_id和serialNumber的restriction

			let data = [];
			let firstId = 0, lastId = 0;
			for (let i = 0; i < req.body.length; i += 1) 
				data.push(Object.values(req.body[i]).slice(1, -1).concat(payload.user_id)); // 有 taskID 要移除

			connectionPool.getConnection(async (connectionError, connection) => {

				if (connectionError) {
					return res.send({ code: 200, message: connectionError }); 
				} else {

					try {
						const queryStatement =
						'INSERT INTO TH928_log (gid, mac, t1, t2, unit, type, serialNumber, datetime, uploadtime, invalid, isDeleted, isSynced, syncTime, user_id) VALUES ?';
							connection.query(queryStatement, [data], (err, result) => {
	
						if (err) { 
								connection.release(); // 釋放連線
								console.log('Failed to add meter log!');
								return res.send({code: 200, message: 'Failed to add meter log!', error: err});

							} else{
								// 從data撈出最後一筆資料，找到最後一筆的ID
								firstId = result.insertId;
								lastId = (result.insertId + result.affectedRows) - 1;

								
								// const last = data[data.length - 1]; 
								// const [rows] = await connection.promise().query('SELECT id FROM TH928_log WHERE user_id = ? and serialNumber = ? and datetime =? and uploadtime = ? LIMIT 1', [payload.user_id, last[0], last[1], last[2]]);

								// 更新 timeout 的 
								const taskId = req.body?.[0]?.taskId;
								onDataReceived(taskId, payload.user_id, firstId, lastId, false); 

								connection.release();
								
								// res.send({ code: 200, message: 'Success', lastId: rows?.[0]?.id ?? 0});
								return res.send({ code: 200, message: 'Success', lastId: lastId}); 
							}
						});

					} catch {
						return res.send({code: 200, message: 'Failed to add meter logs!', error: err});
					}
				}
			});
		}
	});
});

/*
	Check data log sync status 
*/
app.post('/api/chy/device/dataLog/ifUploadSync', function (req, res) {
	console.log('Check if data log sync');

	let { syncTime } = req.body; 

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection( async (connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError });
				} else { 
					const queryStatement = `SELECT id FROM TH928_log WHERE user_id = ? and syncTime > ? ORDER BY id LIMIT 201`;
				
					connection.query(queryStatement, [payload.user_id, syncTime], (err, result) => {
						if (err) {
							res.send({
								code: 200,
								message: 'Unable to check synced status',
								error: err,
							});
							console.log('Unable to check synced status');
						} else {
							res.send({
								code: 200, 
								message: 'Successfully check synced status!',
								needSync: result.length > 0 ? true : false, // 檢查到有資料就同步 
								num: result.length == 201 ? result.length - 1 : result.length,
								hasMoreData: result.length == 201 ? true : false,
								firstId: result.length > 0 ? result[0]?.id : -1,
								currentSyncTime: Date.now(),
							});
						}
						connection.release(); // 釋放連線
					});
				}
			});
		}
	});
});

/*
	Data log sync
*/
app.post('/api/chy/device/dataLog/uploadSync', function (req, res) { // login
	console.log('Data log sync'); 

	let { nextId, lastSyncTime, currentSyncTime } = req.body;

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError });
				} else {

					const queryStatement = `SELECT id, gid, mac, t1, t2, unit, type, serialNumber, datetime, uploadtime, invalid, isDeleted, isSynced, syncTime FROM TH928_log WHERE user_id = ? and syncTime > ? and syncTime <= ? and id >= ? ORDER BY id LIMIT 201`;
					connection.query(
						queryStatement,
						[payload.user_id, lastSyncTime, currentSyncTime, nextId], 
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'Unable to check synced status',
									error: err,
								});
								console.log('Unable to check synced status');
							} else {
								const hasExtra = result.length == 201;
								res.send({
									code: 200,
									message: 'Successfully check synced status!',
									needSync: hasExtra ? true : false,
									num: hasExtra ? 200 : result.length,
									nextId: hasExtra ? result[200].id : null,
									data: hasExtra ? result.slice(0, 200) : result, 
								});
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*
	Check delete data log sync status 
*/
app.post('/api/chy/device/dataLog/ifDeleteSync', function (req, res) { 
	console.log('Check is data log sync');

	let { isDeleted } = req.body;

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError });
				} else {
					const queryStatement = `SELECT id FROM TH928_log WHERE user_id = ? and isDeleted > ? ORDER BY id LIMIT 201`;
					connection.query(
						queryStatement,
						[payload.user_id, isDeleted],
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'Unable to check synced status',
									error: err,
								});
								console.log('Unable to check synced status');
							} else {
								res.send({
									code: 200,
									message: 'Successfully check synced status!',
									needSync: result.length > 0 ? true : false, 
									num: result.length == 201 ? result.length - 1 : result.length,
									hasMoreData: result.length == 201 ? true : false,
									firstId: result[0]?.id ?? null,
								});
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*
	Delete data log sync
*/
app.post('/api/chy/device/dataLog/delSync', function (req, res) {
	console.log('Delete data log sync');

	let { nextId, lastIsDeleted, currentSyncTime, taskId} = req.body;

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError });
				} else {
					const queryStatement = `
							SELECT id, gid, serialNumber, datetime, uploadtime, isDeleted 
						 	FROM TH928_log 
						 	WHERE user_id = ? and isDeleted > ? and isDeleted <= ? and id >= ? 
						 	ORDER BY id LIMIT 201
						`;
					connection.query(
						queryStatement,
						[payload.user_id, lastIsDeleted, currentSyncTime, nextId], 
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'Unable to check synced status',
									error: err,
								});
								console.log('Unable to check synced status');
							} else {
								const hasExtra = result.length == 201;
								res.send({
									code: 200,
									message: 'Successfully check synced status!',
									needSync: hasExtra ? true : false,
									num: hasExtra ? 200 : result.length,
									nextId: hasExtra ? result[200].id : null,
									data: hasExtra ? result.slice(0, 200) : result,
								});
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});

			// 更新 timeout 的 timer 
			refreshTimer(taskId, payload.user_id); 
		}
	});
});

/*
	Update isDeleted from local pc
*/
app.post('/api/chy/device/dataLog/updateDelSync', function (req, res) {
	console.log('Upload data from meter');

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			//const isDeleted = Date.now();
			let data = [];

			for (let i = 0; i < req.body.length; i += 1){ 
				// data.push(Object.values(req.body[i]).concat([payload.user_id])); // 原
				data.push(Object.values(req.body[i]).slice(1, -1).concat(payload.user_id)); // 新 
			}

			connectionPool.getConnection(async (connectionError, connection) => { 
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					console.log(data);
					const values = data
						.map(
							(item) =>
								`('${item[0]}', ${item[1]}, ${item[2]}, ${item[3]}, ${item[4]})`
						)
						.join(',\n');

					// 分成 4 步執行
					try {
						// 1. 建立臨時表
						await connection.promise().query(`
							CREATE TEMPORARY TABLE temp_sync_updates (
								serialNumber VARCHAR(50),
								\`datetime\` BIGINT,
								uploadtime BIGINT, 
								isDeleted BIGINT,
								user_id BIGINT,
								PRIMARY KEY (serialNumber, \`datetime\`, uploadtime, user_id)
							)
						`);

						// 2. 插入資料
						await connection.promise().query(`
							INSERT INTO temp_sync_updates VALUES ${values}
						`); 

						// 3. 更新
						await connection.promise().query(`
							UPDATE TH928_log t
							INNER JOIN temp_sync_updates u 
								ON t.serialNumber = u.serialNumber 
								AND t.\`datetime\` = u.\`datetime\`
								AND t.uploadtime = u.uploadtime
								AND t.user_id = u.user_id
							SET t.isDeleted = u.isDeleted
						`);

						// 4. 清理（可選，連線關閉時會自動清理）
						await connection.promise().query(`DROP TEMPORARY TABLE temp_sync_updates`);
						
						// 5.最後一筆 deleted 的 ID
						const last = data[data.length - 1]; 
						const [rows] = await connection.promise().query('SELECT id FROM TH928_log WHERE user_id = ? and serialNumber = ? and \`datetime\` = ? and uploadtime = ? and isDeleted = ? LIMIT 1', [payload.user_id, last[0], last[1], last[2], last[3]]);

						return res.send({
							code: 200,
							message: 'Success',
							lastId: rows?.[0]?.id ?? 0
						});
					} catch (error) {
						return res.send({
							code: 200, // 
							message: 'Unable to check synced status',
							error: error,
						});
						console.error('更新失敗:', error);
					}
				}
			});
			
			// 更新 timeout
			var taskId = req.body[0].taskId; 
			refreshTimer(taskId, payload.user_id); 
		}
	});
});

/*
	checkLastLogSyncTime
*/
app.post('/api/chy/device/dataLog/checkLastLogSyncTime', function (req, res) {
	console.log('Delete data log sync');

	let { lastSyncTimeId, lastSynctime} = req.body; 

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError });
				} else {
					const queryStatement = `
						SELECT id, gid, mac, t1, t2, unit, type, serialNumber, datetime, uploadtime, invalid, isDeleted, isSynced, syncTime 
						FROM TH928_log WHERE user_id = ? and syncTime <= ?  and id >= ? 
						ORDER BY id 
						LIMIT 201`;
					connection.query(
						queryStatement,
						[payload.user_id, lastSynctime, lastSyncTimeId], 
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'Unable to check synced status',
									error: err,
								});
								console.log('Unable to check synced status');
							} else {
								const hasExtra = result.length == 201;
								res.send({
									code: 200,
									message: 'Successfully check synced status!',
									needSync: hasExtra ? true : false, 
									num: hasExtra ? 200 : result.length,
									nextId: hasExtra ? result[200].id : null,
									data: hasExtra ? result.slice(0, 200) : result,
								});
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*
	checkLastLogisDeleted
*/
app.post('/api/chy/device/dataLog/checkLastLogisDeleted', function (req, res) {
	console.log('Delete data log sync');

	let { lastIsDeletedId, lastIsDeleted} = req.body; 

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError });
				} else {
					const queryStatement = `
						SELECT id, gid, mac, t1, t2, unit, type, serialNumber, datetime, uploadtime, invalid, isDeleted, isSynced, syncTime 
						FROM TH928_log WHERE user_id = ? and syncTime <= ?  and id >= ? 
						ORDER BY id 
						LIMIT 201`;
					connection.query(
						queryStatement,
						[payload.user_id, lastIsDeleted, lastIsDeletedId], 
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'Unable to check synced status',
									error: err,
								});
								console.log('Unable to check synced status');
							} else {
								const hasExtra = result.length == 201;
								res.send({
									code: 200,
									message: 'Successfully check synced status!',
									needSync: hasExtra ? true : false,
									num: hasExtra ? 200 : result.length,
									nextId: hasExtra ? result[200].id : null,
									data: hasExtra ? result.slice(0, 200) : result,
								});
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		}
	});
});

/*
	TaskEnd information
*/
app.post('/api/chy/dataSync/ifTaskEnd', function (req, res) {
	console.log('ifTaskEnd');

	let { taskId, ifEnd } = req.body; 

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			if(ifEnd){
				clearTimer(taskId, payload.user_id); 

				res.send({
							code: 200,
							message: 'Success',
						});
			} else {
				// 不清除timer => 觸發timeout(代表同步失敗) => 刪除 local端上傳的資料
				res.send({
							code: 200,
							message: 'Trigger timeout', 
				});			
			}
		}
	});
});

/*
    Upload the file to the S3， 檔案上傳後會暫存到 EC2執行個體上的 ./tmp_uploads資料夾，再搬到 S3 的 ./uploads/[username]/[extName]/
*/
app.post('/api/chy/device/s3file', function (req, res) {
	console.log("Upload user's file!");

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功

			//上傳檔案保留副檔名、設定最大檔案數、設定檔案上傳後，檔案暫存的資料夾，之後會從這個資料夾把檔案寫入目標的資料夾
			const form = new formidable.IncomingForm({
				keepExtensions: true,
				maxFieldsSize: 20 * 1024 * 1024,
				uploadDir: __dirname + '/tmp_uploads/',
				multiples: true,
			});
			form.parse(req, (err, fields, files) => {
				//上傳檔案，到本地的./upload/username/extName(檔名) 資料夾
				if (err) {
					// 可能遇到網路或其它問題，導致無法開啟檔案
				} else if (Object.keys(files).length == 0) {
					// 送過來的檔案是空的，使用者可能沒有選擇檔案
					res.send({ code: 200, message: 'Cannot be an empty file' });
				} else {
					// 成功解析上傳的檔案，進行讀寫

					const oldPath = files.file[0].filepath; // 檔案暫存的資料夾在 uploadDir:__dirname +"/tmp_uploads/"
					const extName = path.parse(files.file[0].originalFilename).ext.replace('.', ''); // 取得副檔名，例如: jpg不是.jpg，副檔名是來自原檔案，不是使用者額外打的fileName
					const rawData = fs.readFileSync(oldPath); // 讀取路徑下的資料
					let newPath = 'uploads/' + payload.username + '/' + extName + '/'; // S3不能有.當資料夾名稱，新檔案要寫入的入徑，__dirname指向目前的路徑
					let newFilename = fields.fileName[0].replace('.' + extName, ''); // 純檔案名稱，沒有副檔名，例如: newPicture
					const UploadTime = new Date().toISOString(); // 宣告存檔時間
					var newFileNameTemp = newFilename; // 如果沒有重複的檔名就和原本的檔名一樣，寫入資料庫時用newFileNameTemp

					let outcome;
					outcome = ifObjectItemIsEmpty({ fileName: fields.fileName[0] }); // 判斷檔名前後有沒空格 or 空字串
					if (outcome.hasErr) {
						return res.send({ code: 200, message: outcome.message });
					}

					// 規範副檔名
					if (
						!(
							extName == 'jpg' ||
							extName == 'jpeg' ||
							extName == 'png' ||
							extName == 'gif' ||
							extName == 'heic'
						)
					) {
						return res.send({ code: '200', message: 'Must be in image format' });
					}

					// // HeadObjectCommand 檢查檔案有沒有存在。如果檔案存在，變更檔名。原始的 S3 儲存會直接覆蓋。但資料庫要覆蓋?
					input = {
						Bucket: BUCKET_NAME, // S3的儲存體名稱
						Key: newPath + newFileNameTemp + '.' + extName, // 檔案
					};

					// HeadObjectCommand 沒找到路徑裡的檔案，會整個程式停掉，所以要做例外處理
					const commands = new HeadObjectCommand(input);

					s3Client.send(commands).then(
						(data) => {
							// 欲存入的檔案存在，改變檔名，在檔名後加上時間
							console.log('file exist, change the file name');

							createTime = formattedTime(); // 產生日期 抓取 ./config/formatDateTime.js 裡的 方法
							newFileNameTemp = newFileNameTemp + '-' + createTime;

							newPath = newPath + newFileNameTemp + '.' + extName; // 串接 檔案位置 / 檔名 / 副檔名

							//  上傳檔案到 AWS S3，使用自訂義的 uploadToS3 函數
							uploadToS3(
								BUCKET_NAME,
								rawData,
								files.file[0].mimetype,
								newFileNameTemp,
								extName,
								newPath,
								files.file[0].size,
								payload.user_id,
								UploadTime,
								res
							);
						},
						(error) => {
							// 欲存入的檔案不存在，不需要改變檔名
							console.log('file not exist');

							newPath = newPath + newFileNameTemp + '.' + extName; // 串接 檔案位置 / 檔名 / 副檔名

							//  上傳檔案到AWS S3，使用自訂義的 uploadToS3 函數
							uploadToS3(
								BUCKET_NAME,
								rawData,
								files.file[0].mimetype,
								newFileNameTemp,
								extName,
								newPath,
								files.file[0].size,
								payload.user_id,
								UploadTime,
								res
							);
						}
					);
				}
			});
		}
	});
});

/*
    Get the file from AWS S3 
*/
app.get('/api/chy/device/s3file/', function (req, res) {
	let { fileName } = req.query; // 取得 req.body 資料
	let fileName_utf8 = urlencode.encode(fileName, 'utf-8'); // 轉成utf8格式，這樣下載圖片的話才不會亂碼，如果不加這行中文檔名得圖片會亂碼

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		// JWT驗證
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功
			user_name = payload.username;

			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement，user_id從JWT拿
					const queryStatement =
						'SELECT path FROM data_uploadS3 WHERE user_id = ? and fileName = ?';
					connection.query(queryStatement, [payload.user_id, fileName], (err, result) => {
						if (err) res.send({ code: 200, message: err }); // sql語法有誤
						else if (result.length == 0) {
							// query結果沒有找到，符合user_id = ? and fileName = ?的資料
							res.send({
								code: 200,
								message:
									"The database doesn't have this file's information for this user",
							});
							console.log('Failed to get data!');
						} else {
							downloadS3Image(BUCKET_NAME, result[0].path, fileName_utf8, res); // 自訂義的上傳的檔案 function
						}
						connection.release(); // 釋放連線
					});
				}
			});
		}
	});
});

/*
    Delete the file from AWS S3 
*/
app.delete('/api/chy/device/s3file', function (req, res) {
	console.log("delete user's file!");

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
	// JWT驗證
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功

			// // 取得檔案名稱及副檔名
			// filesname = req.query.fileName; // 這是個 filename 陣列

			// let ext = new Array(req.query.fileName.length);
			// let deleteFilePath = new Array(req.query.fileName.length);

			// for( i=0; i<req.query.fileName.length; i++)
			// {
			//   ext[i] = path.parse(filesname[i]).ext.replace(".",""); // 副檔名
			//   deleteFilePath[i] = "uploads/"+payload.username+"/"+ext[i]+"/"+filesname[i]; // 刪除檔案的路徑
			// }

			filesname = req.query.fileName;

			// 這是個 filename 陣列
			// 1.一個物件 (dog.jpg)             => filesname[0] = "d"
			// 2.二個以上: (dog.jpg、bird.jpg)  => filesname[0] = "dog.jpg"
			// 判斷方法(不確定有沒有 b u g)      => 檢查 filesname[0].length > 1 && filesname[0].includes(".")

			if (filesname[0].length == 1) {
				filesname = [filesname];
			}

			let ext = new Array(req.query.fileName.length);
			let deleteFilePath = new Array(req.query.fileName.length);

			for (i = 0; i < filesname.length; i++) {
				ext[i] = path.parse(filesname[i]).ext.replace('.', ''); // 副檔名
				deleteFilePath[i] =
					'uploads/' + payload.username + '/' + ext[i] + '/' + filesname[i]; // 刪除檔案的路徑
			}

			console.log(deleteFilePath);

			deleteS3Image(BUCKET_NAME, deleteFilePath, payload, res); // 使用自訂義的 function 刪除檔案
		}
	});
});

/*
    Show the file from AWS S3 
*/
app.get('/api/chy/device/s3file/show', function (req, res) {
	let { fileName } = req.query; // 取得 req.body 資料
	let fileName_utf8 = urlencode.encode(fileName, 'utf-8'); // 轉成utf8格式，這樣下載圖片的話才不會亂碼，如果不加這行中文檔名得圖片會亂碼

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		// JWT 驗證
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT 驗證成功
			user_name = payload.username;

			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement，user_id從JWT拿
					const queryStatement =
						'SELECT path FROM data_uploadS3 WHERE user_id = ? and fileName = ?';
					connection.query(queryStatement, [payload.user_id, fileName], (err, result) => {
						if (err) res.send({ code: 200, message: err }); // sql語法有誤
						else if (result.length == 0) {
							// query結果沒有找到，符合user_id = ? and fileName = ?的資料
							res.send({
								code: 200,
								message:
									"The database doesn't have this file's information for this user",
							});
							console.log('Failed to get data!');
						} else {
							showS3Image(BUCKET_NAME, result[0].path, res); // 自訂義的上傳的檔案 function
						}
						connection.release(); // 釋放連線
					});
				}
			});
		}
	});
});

/*
    List all uploaded files of the user by user_id in JWT
*/
app.get('/api/chy/device/s3file/list', function (req, res) {

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 

	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		// JWT驗證
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// JWT驗證成功

			connectionPool.getConnection((connectionError, connection) => {
				// 資料庫連線
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement，user_id從JWT拿
					const queryStatement =
						"SELECT fileName, path, fileSize, UploadTime FROM data_uploadS3 WHERE user_id = ? and path LIKE 'uploads/%' ORDER BY UploadTime DESC";
					connection.query(queryStatement, [payload.user_id], (err, result) => {
						let totalFileSize = 0; // 計算總檔案大小
						for (let i = 0; i <= Object.keys(result).length - 1; i++) {
							// 累加檔案大小
							totalFileSize += result[i].fileSize;
						}

						let totalFileSizes = changeFileSizeFormat(totalFileSize); // 轉換總檔案大小的單位，最大到TB

						if (err) {
							res.send({
								code: 200,
								message: 'Failed to get data list!',  
								error: err,
							});
							console.log('Failed to get data list!');
						} else {
							res.send({
								code: 200,
								message: 'success!',
								number_of_files: Object.keys(result).length,
								total_fileSize: totalFileSizes,
								data: result,
							});
						}
						connection.release(); // 釋放連線 
					});
				}
			});
		}
	});
});

// const app = express()，告訴express伺服器監聽8880 port
app.listen(config.port, () => {
	console.log(`server started on port ${config.port}`);
	// termPeriodTrade("5b867d0a0475571b970046486d033987efcb45d6eafc6d04b5349f01aeb673c473bd5927bc454f569eea09c0cde92a329edfef1d4747ecc8ead13e4c55204faf9da2df9f850d5da1a204f841dbfcd7070f06d1388935a48d7b37fb059574048c3d0c304eacee0440cbc004560843e30c7eb167ce5c08cf683989bfc67f5b3bfce0e64b6022a13543f1633cba32ba4c38df52d5ac7f81eb24c777cfe5c1187ab402a1f573cdf32e42f2491c50d837d2221729d3166a36b1d368b7309b8d72fb073ce496f11bda67c2f4182674a48151b0d942125b32c0e2edef31dfc7e30932fd04f4ed9c0e74b24b918f92e2c211bbc4bfe9ab54a0e29a828c3c0f59c9382ae2");
});

const wss = new WebSocketServer({ host: '0.0.0.0', port: 8885}, () => { 
  console.log(`WebSocket run on: ws://0.0.0.0:8885`); 
}); 

wss.on('connection', (ws) => {
	console.log('new client');
	let currentUserId = null;

	ws.on('message', (message) => {
		console.log('Received: ', message.toString()); 
		const str = message.toString();
		
		if (typeof str === "string" && str.startsWith("chyuid.")) {
			currentUserId = str.split(".")[1]; 
			console.log(`set user: ${currentUserId}`)
			clientMap.set(currentUserId, ws); // 保存用戶的 client 
		}  
	});

	ws.on('close', () =>{ // 連結關閉，刪除 Map 紀錄
		console.log("client disconnected");
		clientMap.delete(currentUserId);
	});

});


/*
    Function 
*/
// 判斷 req.body 裡的 欄位數值 是不是空字串
// 帳號大於6 密碼大於8 
//ifObjectLengthQualify(req.body, ['username', 'password'], [6, 8]);
function ifObjectLengthQualify(object, keyArray, numArray){
	for(let i = 0; i < keyArray.length; i++){
		if(object[keyArray[i]].length < numArray[i])
			return { hasErr: true, message: `${keyArray[i]}  must be greater than ${numArray[i]}` }
	}
	return { hasErr: true, message: "OK" }
}

function ifObjectItemIsEmpty(object) { 
	for (let key in object) {

		if (object[key] === '') { // 如果是空字串
			return { hasErr: true, 
					 message: `${key} can't be empty` };

		} else if (typeof object[key] == 'string') { // 如果不是空字串

			if (object[key] == 0) { // 如果字串是純空格 
				return { hasErr: true, 
						 message: `${key} can't be a space` };

			} else if (!(object[key] == object[key].replace(/(^[\s]*)|([\s]*$)/g, ''))) // 移除 "字串" 前後的 space != 原本的字串
				return { hasErr: true, 
						 message:`No space before or after the ${key}` }; // 字串前後有 space

			else {
				return { hasErr: false, message: 'success'}; 

			}
		} else return {hasErr: true, message: 'error'}; // 都沒問題，回傳 false 
	}
}

// 判斷 req.body 的 username、 password 等.. 有無空格
function ifObjItemSpace(Object, keyArray) {
	console.log('In ifObjItemSpace Function');

	for (let item in keyArray) {
		if (!(Object[keyArray[item]].indexOf(' ') == -1)) { 
			console.log('has space');
			return {hasErr: true, message: `${keyArray[item]} can't contain space!`}
			
		} else {
			return {hasErr: false, message: 'Success'};
		}
	}
	return {hasErr: false, message: 'Success'};
}

// 改變檔案的大小的格式，看在存檔時要不要直接改成適合的MB或KB存，不然就KB存拿出來再轉換
function changeFileSizeFormat(fileSize) {
	// fileSize 的單位是 bytes，資料庫裡存的都是bytes
	var size = '';

	if (fileSize < 1024) {
		size = fileSize + ' Bytes';
	} else if (fileSize < Math.pow(1024, 2)) {
		size = (fileSize / 1024).toFixed(2) + ' KB';
	} else if (fileSize < Math.pow(1024, 3)) {
		size = (fileSize / Math.pow(1024, 2)).toFixed(2) + ' MB';
	} else if (fileSize < Math.pow(1024, 4)) {
		size = (fileSize / Math.pow(1024, 3)).toFixed(2) + ' GB';
	} else if (fileSize < Math.pow(1024, 5)) {
		size = (fileSize / Math.pow(1024, 4)).toFixed(2) + ' TB';
	}
	return size;
}

// 上傳檔案到 AWS S3
const uploadToS3 = async (
	BUCKET_NAME,
	rawData,
	mimetype,
	newFileNameTemp,
	extName,
	newPath,
	size,
	user_id,
	UploadTime,
	res
) => {
	const command = new PutObjectCommand({
		// AWS PutObjectCommand
		Bucket: BUCKET_NAME,
		ContentEncoding: 'utf-8',
		Key: newPath, // 儲存在S3上的檔案名稱，可以加入路徑，且會自動建立其中必要的資料夾。這樣儲存的檔案路徑，如果後續不想用S3，要搬檔案到EC2上存，還能沿用相同的路徑
		Body: rawData, // 檔案
		ContentType: mimetype, // 檔案類型
	});

	await s3Client.send(command).then(
		(data) => {
			// 等待上傳完，開啟資料庫寫入資料庫 or 同步資料庫讀寫

			connectionPool.getConnection((connectionError, connection) => {
				if (connectionError) {
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					// queryStatement，user_id從JWT拿
					const queryStatement =
						'INSERT INTO data_uploadS3 (fileName, path, fileSize, user_id, UploadTime) VALUES (?,?,?,?,?)';
					connection.query(
						queryStatement,
						[newFileNameTemp + '.' + extName, newPath, size, user_id, UploadTime],
						(err, result) => {
							if (err) {
								res.send({
									code: 200,
									message: 'Failed to upload data!',
									error: err,
								});
								console.log('Failed to upload data!');
							} else {
								res.send({
									code: 200,
									message: 'Upload data success!',
									result: result,
								});
								console.log('Upload data success');
							}
							connection.release(); // 釋放連線
						}
					);
				}
			});
		},
		(error) => {
			// send error
			return res.send({ code: 200, message: 'Failed to save the file', error: error }); // 寫入檔案失敗
		}
	);
};

// 從 AWS S3 下載圖片
const downloadS3Image = async (BUCKET_NAME, path, fileName_utf8, res) => {
	const command = new GetObjectCommand({
		// GetObjectCommand 是操作 AWS S3的API
		Bucket: BUCKET_NAME,
		Key: path, // 要下載的檔案
	});

	try {
		const response = await s3Client.send(command);
		// The Body object also has 'transformToByteArray' and 'transformToWebStream' methods.
		const data = await response.Body.transformToByteArray(); // The Body object also has 'transformToByteArray' and 'transformToWebStream' methods.
		res.setHeader('content-disposition', "attachment; filename*=utf-8''" + fileName_utf8); // 設定標頭為檔案下載，以及編碼方式
		res.contentType('image/png'); // 設定contentType為圖片
		res.end(data); // 傳送資料
		// res.writeHead(200, {'Content-Type':'image/png'}) // write header
		// res.end(data) // Send the file data to the browser
	} catch (err) {
		console.log(err);
		res.send({ code: 200, message: 'cannot get the file', err: err });
	}
};

const deleteS3Image = async (BUCKET_NAME, paths, payload, res) => {
	// 當只有一個元素的陣列字串，丟過來變字串，["new.jpg"] = > "new.jpg"
	if (paths[0].length == 1) {
		paths = [paths];
	}
	let itemCount = paths.length;
	let count = 0;

	paths.forEach(async (path) => {
		count++;
		try {
			let fileName = path.replace('uploads/theUser/jpg/', '');

			const command = new DeleteObjectCommand({
				Bucket: BUCKET_NAME,
				Key: path,
			});
			await s3Client.send(command);

			const connection = await new Promise((resolve, reject) => {
				connectionPool.getConnection((err, connection) => {
					if (err) {
						reject(err);
					} else {
						resolve(connection);
					}
				});
			});
		} catch (error) {
			console.error(error);
			return res.send({
				code: 200,
				message: 'Failed to delete the file or data!',
				error: error,
			});
		}
	});

	console.log(paths);

	connectionPool.getConnection((connectionError, connection) => {
		// 資料庫連線
		if (connectionError) {
			res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
		} else {
			// queryStatement，user_id從JWT拿
			const queryStatement = 'DELETE FROM data_uploadS3 WHERE user_id = ? and path IN (?)';
			connection.query(queryStatement, [payload.user_id, paths], (err, result) => {
				if (err) {
					res.send({ code: 200, message: 'Failed to delete data!', error: err });
					console.log('Failed to delete data!');
				} else {
					res.send({ code: 200, message: 'Delete data success!' });
					console.log('Delete data success!');
				}
				connection.release(); // 釋放連線
			});
		}
	});
};

// 顯示 AWS S3 上的圖片
const showS3Image = async (BUCKET_NAME, path, res) => {
	const command = new GetObjectCommand({
		// GetObjectCommand 是操作 AWS S3的API
		Bucket: BUCKET_NAME,
		Key: path, // 要下載的檔案
	});

	try {
		const response = await s3Client.send(command);
		// The Body object also has 'transformToByteArray' and 'transformToWebStream' methods.
		const data = await response.Body.transformToByteArray(); // The Body object also has 'transformToByteArray' and 'transformToWebStream' methods.
		res.writeHead(200, { 'Content-Type': 'image/png' }); // write header
		res.end(data); // Send the file data to the browser
	} catch (err) {
		console.log(err);
		res.send({ code: 200, message: 'cannot get the file', err: err });
	}
};


// 全域 timers map 一群 timer 
// 1.這裡沒有 cloud 沒有還原 isDeleted = 1 AND syncTime > 0，Local端也會留著，下次同步繼續刪，同步表的紀錄會還原到上傳前
// 2.cloud 的 local 上傳資料會刪除 3.local的 上傳資料 syncTime 歸零 4.local的雲端下載刪除 
// local 的刪除不還原(存進 cloud DB 和 local DB 修改) 

const timers = new Map();
const LogUpload = new Map();
const TmpUpload = new Map();

function getKey(taskId, userId) { 
  return `${taskId}_${userId}`; ; 
}

function refreshTimer(taskId, userId) {
  const keyValue = `${taskId}_${userId}`;

  // 清掉舊的 timer
  clearTimeout(timers.get(keyValue)); 

  // 重新計時
  const id = setTimeout(() => { 
    console.log(`userId:${userId} taskId:${taskId} timeout!`);
    timers.delete(keyValue);
    handleTimeout(taskId, userId, keyValue)
  }, 15000); 

  timers.set(keyValue, id);
}

// 收到資料時呼叫
function onDataReceived(taskId, userId,start, end, IfEnds) {

  addData(taskId, userId, start, end); 
  
  // 到盡頭
  if(IfEnds){
	clearTimer(taskId, userId);
  } else {
	refreshTimer(taskId, userId); 
  }
}

// timeout處理
function handleTimeout(taskId, userId, str) {
  console.log(`任務 ${taskId} 使用者 ${userId} 已斷線`); 
  getMergedRanges();
  
  // 檢查 taskId
  if (str.startsWith('TemplateUp_')) { // 復原 本地端傳過來的 刪除資料

	} else if (str.startsWith('LogUp_')) { // 復原 本地端傳過來的 上傳資料
		recoveryLocalLogUp(taskId, userId);
	} 

	remove(getKey(taskId, userId)); // 刪掉暫存 對應 key 的 cloudDB id 資料
}

function recoveryLocalLogUp(taskId, userId){

	// 取出 taskId 的資料
	var data =  getMergedRanges(taskId, userId); 
	
	const resultObj = buildDeleteSQL(data, userId); 
	if (!resultObj) return;

	const { sql, params } = resultObj;

	connectionPool.getConnection((connectionError, connection) => {
		// 資料庫連線
		if (connectionError) {
			console.log('Failed to delete data!'); 
		} else { 
			connection.query(sql, params, (err, result) => {
				if (err) {
					console.log('Failed to delete data!');
				} else {
					console.log('Delete data success!');
				}
			});
		}
		connection.release(); // 釋放連線 
	});
}

function recoveryLocalTmpUp(taskId, userId){
	

}

// 手動清除某個 timer（例如任務完成）
function clearTimer(taskId, userId) { 
  const key = `${taskId}_${userId}`;
  clearTimeout(timers.get(key));
  timers.delete(key); 
}

// 1.存在 LogUpload 裡，時間到要刪除 LogUpload 裡對應的資料
// 2.需要復原刪除 timer，撈出資料處理資料庫，再從 LogUpload 裡移除
// 3.merge方法看一下、get方法=>還原資料庫
// 4.如何銜接? 與原API
// 目前無法確認 刪除(start, end相同) 或 批量連續上傳(有start, end)
// 每發一筆都要空刷新嗎? 要有一個可以純刷新 timer 的 被報 
// 不傳都由server產生與管理，只要有刷新的機制 up_uuid, 刪除 del_uuid
// 最後一個結尾封包 停掉 timeout => 刪除 Map 的 timer => 處理資料庫 => 刪除 LogUpload
// 本地上傳，本地刪除( 單一筆的如何合併 有 del LogUpload?) 手法一樣 or到底? 字串太長要考慮 因為條件有很多 有區間 又有單筆 改怎麼處理
// 處理刪除 條件 
function addData(taskId, userId, start, end) {  
  const key = `${taskId}_${userId}`; 
  let item = LogUpload.get(key); 

  // 1.增加初始物件
  if (!item) { 
      const timer = setTimeout(() => { 
        console.log('timeout:', key); 
        LogUpload.delete(key); 
      }, 20000); 
	
      item = { timer,  ranges: [] }; 
  } else {
      item.timer.refresh(); 
  }
  
  // 2.增加資料
  if(taskId.startsWith("LogUp_")){ 
      LogUpload.set(key, item); 
	  item.ranges.push({ start, end }); 
  }
  else if(taskId.startsWith("TmplateUp_")) {
	  TmpUpload.set(key, item); 
	  item.ranges.push(...start); // ... 展開陣列 
  }
}


// 合併區間
// [
//   {start: 0, end: 100},
//   {start: 101, end: 200}
// ]
// =>[{start: 0, end: 200}]
function mergeRanges(ranges) {
  if (!ranges || ranges.length === 0) return [];

  // 1. 排序
  ranges.sort((a, b) => a.start - b.start);

  const result = [ranges[0]];

  for (let i = 1; i < ranges.length; i++) {
    const last = result[result.length - 1];
    const current = ranges[i];

    // 2. overlap 判斷
    if (current.start <= last.end) {
      // 3. merge
      last.end = Math.max(last.end, current.end);
    } else {
      // 4. 不重疊
      result.push(current); 
    }
  }

  return result; 
}

// 取得 + merge
function getMergedRanges(taskId, userId) { 
  const key = getKey(taskId, userId);
  const item = LogUpload.get(key);

  if (!item) return [];

  return mergeRanges(item.ranges);
}

// 刪除 LogUpload變數 的資料
function remove(key) {
  const item = LogUpload.get(key); // 4327:51
  if (!item) return;

  // 清 timer
  if (item.timer) {
    clearTimeout(item.timer);
  }

  // 清掉內部資料 
  item.ranges = null;

  // 從 Map 移除 reference 
  LogUpload.delete(key);
}

// 時戳 + randomBytes + user_id 產生訂單編號
function genRandString(user_id) {
  return `${Date.now()}${crypto.randomBytes(2).toString('hex')}_${user_id}`;
}

function getUidFroMNO(MerchantOrderNo){
	return MerchantOrderNo.split("_")[1]; // 取得 user_id
}   

// 藍新將請求字串加密 
app.post('/api/chy/neWebpPay/encryption', function (req, res) { 
	console.log('/api/chy/neWebpPay/encryption');

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
					
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			const keyString = config.HASH_KEY;
			const ivString = config.HASH_IV; 
			const midString = config.MERCHANT_ID;
			const version = config.NB_VERSION; 

			const algorithm = "aes-256-cbc";
			const keyBytes = Buffer.from(keyString, "utf8"); // 可操作二進位資料 
			const ivBytes = Buffer.from(ivString, "utf8");   
			const midBytes = Buffer.from(midString, "utf8"); 

			// 1. 生成請求字串 
			const params = new URLSearchParams({ 
				MerchantID: midString,
				RespondType: "String",
				TimeStamp:  Math.floor(Date.now() / 1000).toString(), // php 的 time() 是自 1970 經過的秒數，Date.now 是毫秒要除1000
				Version: version,
				MerchantOrderNo:  genRandString(payload.user_id), 
				Amt: 30, 
				ItemDesc: "test", 
				CREDIT: 1, // 信用卡一次付清 
				LangType: "en", 
				//NotifyURL: config.NB_NOTIFY_URL // 只接受 80 或 443 port *所以這裡要改 
				
			}); 

			// 由二進位轉16進位
			// $edata1=bin2hex(openssl_encrypt($data1, "AES256 CBC", $key,OPENSSL_RAW_DATA, $iv);
			
			// 2.1 建立帶有 iv(初駛向量) 的 加密器 
			const cipher = crypto.createCipheriv(
				algorithm, // AES 加密 
				keyBytes,
				ivBytes
			); 

			// 2.2 請求字串加密 
			let edata = cipher.update(params.toString(), "utf8", "hex"); // 原始資料-輸入格式-輸出格式
			edata += cipher.final("hex"); // 加密區塊大小為 16bytes(256byte) 所以如果資料是 20bytes，拆出來會是16+4，這個4bytes，final()會 補齊基本拆解的大小 16bytes

			// 3. AES 加密字串產生檢查碼 (要注意utf-8)
			// $hashs="HashKey=".$key."&".$edata1."&HashIV=".$iv; 
			const hashs = `HashKey=${keyString}&${edata}&HashIV=${ivString}`; 

			// 4. 將字串轉 sha256 & 大寫 
			const hash = crypto.createHash("sha256").update(hashs).digest("hex").toUpperCase(); 
			
			// 5.發布字串 這裡由 JS 去做  
			// MerchantID、TradeSha、TradeInfo 
			res.send({ code: 200, message: 'Success', merchantID: midString, hash: hash, edata: edata, version: version}); 
					
		}
	});
});

app.post('/api/chy/neWebpPay/sub_encryption', function (req, res) {
	console.log('/api/chy/neWebpPay/sub_encryption');

	const { email, periodTimes } = req.body;
	
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
					
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			const keyString = config.HASH_KEY;
			const ivString = config.HASH_IV; 
			const midString = config.MERCHANT_ID;
			const version = config.NB_SUB_VERSION; 

			const algorithm = "aes-256-cbc";
			const keyBytes = Buffer.from(keyString, "utf8"); // 可操作二進位資料 
			const ivBytes = Buffer.from(ivString, "utf8"); 
			const midBytes = Buffer.from(midString, "utf8"); 

			const date = new Date();
			date.setDate(date.getDate() + 30);

			const PeriodFirstdate =`${date.getFullYear()}/
			${String(date.getMonth() + 1).padStart(2, '0')}/ 
			${String(date.getDate()).padStart(2, '0')}`; 
			
			// 
			// 1. 生成請求字串 
			const params = new URLSearchParams({ 
				RespondType: "String",
				TimeStamp:  Math.floor(Date.now() / 1000).toString(), // php 的 time() 是自 1970 經過的秒數，Date.now 是毫秒要除1000
				Version: version,
				LangType: "en", 
				MerOrderNo:  genRandString(payload.user_id), 
				ProdDesc: "TH928 pc sw subscription",
				PeriodAmt: 30, 
				PeriodType: "M",
				PeriodPoint: "05",
				PeriodStartType: 3, 
				PeriodTimes: periodTimes, 
				PayerEmail: email, 
				PeriodFirstdate: PeriodFirstdate, 
				EmailModify: 1,
				PaymentInfo: "N",
				OrderInfo: "N", 
				NotifyURL: config.NB_NOTIFY_URL // 只接受 80 或 443 port  
			}); 

			// 2.將請求字串加密
			// 2.1 建立帶有 iv(初駛向量) 的 加密器 
			const cipher = crypto.createCipheriv( 
				algorithm, // AES 加密 
				keyBytes,
				ivBytes, 
			); 

			// 2.2 請求字串加密 
			let edata = cipher.update(params.toString(), "utf8", "hex"); // 原始資料-輸入格式-輸出格式
			edata += cipher.final("hex"); // 加密區塊大小為 16bytes(256byte) 所以如果資料是 20bytes，拆出來會是16+4，這個4bytes，final()會 補齊基本拆解的大小 16bytes

			// 3.發布字串 這裡由 JS 去做 
			res.send({ code: 200, message: 'Success', merchantID: midString, edata: edata }); 

		}
	});
}); 

// 獲得日期最近的訂閱訂單的資料，並返回，並取消 
app.post('/api/chy/neWebPay/getPeriodTradInfo', function (req, res) { 
	console.log("getPeriodInfo");
	
	const token = typeof req.headers.authorization === 'string' 
						? req.headers.authorization.split(' ')[1] 
						: null; 
					
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => { 
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			
			connectionPool.getConnection((connectionError, connection) => {
				if (connectionError) { 
					res.send({ code: 200, message: connectionError }); // mysql連線有問題回傳錯誤
				} else {
					
					const queryStatement = 'SELECT * FROM trade_info WHERE user_id = ? ORDER BY id DESC LIMIT 1';
					connection.query(queryStatement, [payload.user_id], (err, result) => {
						if (err) {
							res.send({ code: 200, message: 'err', error: err });
						} else { 
							res.send({
								code: 200, 
								message: 'Success', 
								data: result
							});
						}
						connection.release(); // 釋放連線
					});
				}
			});
		}
	});
}); 

app.post('/api/chy/neWebpPay/term_sub_encryption', function (req, res) {
	console.log('/api/chy/neWebpPay/sub_encryption');

	const { periodNo, merchantOrderNo } = req.body;
	
	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
					
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			const keyString = config.HASH_KEY;
			const ivString = config.HASH_IV; 
			const midString = config.MERCHANT_ID;
			const version = config.NB_TERM_SUB_VERSION; 

			const algorithm = "aes-256-cbc";
			const keyBytes = Buffer.from(keyString, "utf8"); // 可操作二進位資料 
			const ivBytes = Buffer.from(ivString, "utf8"); 
			const midBytes = Buffer.from(midString, "utf8"); 
			
			// 1. 生成請求字串 
			const params = new URLSearchParams({ 
				RespondType: "String",
				Version: version,
				MerOrderNo: merchantOrderNo,
				PeriodNo: periodNo, 
				AlterType: "terminate", 
				TimeStamp: Math.floor(Date.now() / 1000).toString(), // php 的 time() 是自 1970 經過的秒數，Date.now 是毫秒要除1000
			}); 

			// 2.將請求字串加密
			// 2.1 建立帶有 iv(初駛向量) 的 加密器 
			const cipher = crypto.createCipheriv( 
				algorithm, // AES 加密 
				keyBytes,
				ivBytes, 
			); 

			// 2.2 請求字串加密 
			let edata = cipher.update(params.toString(), "utf8", "hex"); // 原始資料-輸入格式-輸出格式
			edata += cipher.final("hex"); // 加密區塊大小為 16bytes(256byte) 所以如果資料是 20bytes，拆出來會是16+4，這個4bytes，final()會 補齊基本拆解的大小 16bytes

			// 3.發布字串 這裡由 JS 去做 
			res.send({ code: 200, message: 'Success', merchantID: midString, edata: edata});  
		}
	});
}); 

app.post('/api/chy/neWebpPay/term_sub', function (req, res) {

	let { period } = req.body;

	const token = typeof req.headers.authorization === 'string'
							? req.headers.authorization.split(' ')[1]
							: null; 
					
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			try{
				var Info = termPeriodTrade(period); 
				res.send({ code: 200, message: 'Success', data: Info });
			} catch {
				res.send({ code: 200, message: 'err', error: err });
			} 
		}
	}); 
});

// 藍新過20天才會自動請款，會有20天的授權狀態，倒數20天自動請款，或是可以在後台手動請款
// 20天之間要退費是用取消授權，因為還沒請款。如果已經請款後，要用退款功能。
app.post('/api/chy/neWebpPay/credit_cancel_authorization', function (req, res) { 

	let { merchantOrderNo, amt } = req.body;

	const keyString = config.HASH_KEY;
	const ivString = config.HASH_IV; 
	const midString = config.MERCHANT_ID; 
	const version = config.NB_VERSION; 

	const algorithm = "aes-256-cbc";
	const keyBytes = Buffer.from(keyString, "utf8"); // 可操作二進位資料 
	const ivBytes = Buffer.from(ivString, "utf8"); 
	const midBytes = Buffer.from(midString, "utf8"); 

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
					
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => { 
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			// 1. 生成請求字串 
			const params = new URLSearchParams({ 
				RespondType: "String",
				Version: version,
				Amt: amt, 
				MerchantOrderNo: merchantOrderNo, 
				TimeStamp: Math.floor(Date.now() / 1000).toString(), // php 的 time() 是自 1970 經過的秒數，Date.now 是毫秒要除1000
				IndexType: 1, // 1 是 MerchantOrderNO，2 是 TradeNo 
			}); 

			// 2.將請求字串加密
			// 2.1 建立帶有 iv(初駛向量) 的 加密器 
			const cipher = crypto.createCipheriv( 
				algorithm, // AES 加密 
				keyBytes,
				ivBytes, 
			); 

			// 2.2 請求字串加密 
			let edata = cipher.update(params.toString(), "utf8", "hex"); // 原始資料-輸入格式-輸出格式
			edata += cipher.final("hex"); // 加密區塊大小為 16bytes(256byte) 所以如果資料是 20bytes，拆出來會是16+4，這個4bytes，final()會 補齊基本拆解的大小 16bytes

			// 3.發布字串 這裡由 JS 去做 
			res.send({ code: 200, message: 'Success', merchantID: midString, edata: edata});
		}
	});
});

// 解密 一次性付款 的 取消授權 
app.post('/api/chy/neWebpPay/decoding_cancel_authorization', function (req, res) { 
	console.log("/api/chy/neWebpPay/decoding_cancel_authorization");

	let { merchantOrderNo, amt, message, status, checkCode, tradeNo } = req.body;

	const keyString = config.HASH_KEY;
	const ivString = config.HASH_IV; 
	const midString = config.MERCHANT_ID; 

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
					
	jwt.verify(token, config.myJWTsecretKey, async (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
			
			// checkCode 解密
			// 1.amt、merchantOrderNo, MerchantID、tradeNo 產生checkCode
			const item = {MerchantID: midString, Amt: amt, MerchantOrderNo: merchantOrderNo, TradeNo: tradeNo}; 
			const genCode = genCheckCode(item, keyString, ivString);
			console.log("genCode: " + genCode); 
			console.log("checkCode: " + checkCode); 

			// 2.驗證兩個 checkCode 是否一樣
			if(genCode == checkCode){
				// 存資料庫
				let connection;
				try {
					connection = await connectionPool.promise().getConnection();
					await connection.beginTransaction(); 

					// 3.寫入交易紀錄（無論成功失敗都記錄），用 ON DUPLICATE KEY UPDATE 防止重複 Notify 造成重複寫入
					// *如果是 PeriodStartType = 3，有 PeriodNo 但沒有 tradeNo，PeriodStartType 1, 2 才是都有
					const insertSql = `
						INSERT INTO credit_cancel_auth 
						(merchantID, merchantOrderNo, status, message, amt, user_id)
						VALUES (?, ?, ?, ?, ?, ?); 
					`;  
					
					await connection.query(insertSql, [ 
						midString,
						merchantOrderNo, 
						status,
						message,
						amt,
						user_id,
					]);

					// 4. 如果 取消授權 成功，更新使用者狀態 
					if (status == 'SUCCESS') {
						await connection.query(
							'UPDATE user_info SET userStatus = ?, endDate = ? WHERE user_id = ?',
							[userStatus.activate, "", user_id] 
						); 
					}

					await connection.commit(); 

					// 5.通知 取消授權是 成功 or 失敗，傳給 WPF，WPF 再給 html，再顯示
					res.send({ code: 200, status: "SUCCESS", message: "Ok"}); // 因為藍新是傳 "SUCCESS"，這裡跟它統一
					
				} catch (err) {
					if (connection) {
						await connection.rollback(); // 失敗全部退回 
					}
					console.error('Failed to process trade notify:', err); 

					// 5.通知 取消授權是 成功 or 失敗，傳給 WPF，WPF 再給 html，再顯示
					res.send({ code: 200, status: "Error", message: "Database connection error"}); 

				} finally { 
					if (connection) {
						connection.release(); 
					}
				}
				
			} else { 
				res.send({ code: 200, status: "Error", message: "error checkCode"}); 
			}

		}
	});
});

// 單一交易紀錄查詢
app.post('/api/chy/neWebpPay/single_query', function (req, res) { 

	let { merchantOrderNo, amt } = req.body; 

	const keyString = config.HASH_KEY;
	const ivString = config.HASH_IV; 
	const midString = config.MERCHANT_ID; 
	const version = config.NB_VERSION; 

	const algorithm = "aes-256-cbc";
	const keyBytes = Buffer.from(keyString, "utf8"); // 可操作二進位資料 
	const ivBytes = Buffer.from(ivString, "utf8"); 
	const midBytes = Buffer.from(midString, "utf8"); 

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
					
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
				var params = {
					MerchantID: midString, 
					Amt: amt,
					MerchantOrderNo: merchantOrderNo, 
				}; 

				const checkValue = genCheckValue(params, keyString, ivString);

			// 3.發布字串 這裡由 JS 去做  
			res.send({ code: 200, message: 'Success', merchantID: midString, version: version, responseType: "String", 
					   checkValue: checkValue, timeStamp: Math.floor(Date.now() / 1000).toString(), merchantOrderNo: merchantOrderNo, amt: amt});
		}
	});
});

// 單一交易紀錄 的 checkCode 驗證是否是對的
app.post('/api/chy/neWebpPay/getCheckCode', function (req, res) { 

	let { merchantOrderNo, amt, tradeNo } = req.body; 

	const keyString = config.HASH_KEY;
	const ivString = config.HASH_IV; 
	const midString = config.MERCHANT_ID; 
	const version = config.NB_VERSION; 

	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
					
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
				var params = {
					MerchantID: midString, 
					Amt: amt,
					MerchantOrderNo: merchantOrderNo, 
					TradeNo: tradeNo, 
				}; 

				const checkCode = genCheckCode(params, keyString, ivString);

			// 3.發布字串 這裡由 JS 去做  
			res.send({ code: 200, message: 'Success', checkCode: checkCode}); 
		}
	});
});


app.post('/api/chy/neWebpPay/getCheckValue', function (req, res) { 

	let { merchantOrderNo, amt } = req.body; 

	const keyString = config.HASH_KEY;
	const ivString = config.HASH_IV; 
	const midString = config.MERCHANT_ID; 


	const token = typeof req.headers.authorization === 'string'
						? req.headers.authorization.split(' ')[1]
						: null; 
					
	jwt.verify(token, config.myJWTsecretKey, (err, payload) => {
		if (err) {
			res.send({ code: 200, message: 'JWT Authentication Failure', error: err });
		} else {
				var params = {
					MerchantID: midString, 
					Amt: amt,
					MerchantOrderNo: merchantOrderNo, 
				}; 

				const checkValue = genCheckValue(params, keyString, ivString);

			// 3.發布字串 這裡由 JS 去做  
			res.send({ code: 200, message: 'Success', checkValue: checkValue, respondType: "String", version: config.NB_SINGLE_QU_VERSION, timeStamp: Math.floor(Date.now() / 1000).toString() }); 
		}
	});
});

// 進入交易頁面後 回傳的 response， 移除 後綴多餘的padding，之後再解密
function stripPadding(buffers) {

  	var buffer = Buffer.from("null"); 

	if(Buffer.isBuffer(buffers)){
		buffer = Buffer.from(buffers); 
	}
	
	if (buffer.length === 0) { 
		return false; 
	}
	
	// 1.ord(substr($string, -1)) -> 取最後一個 byte 的數值 
	const slast = buffer[buffer.length - 1]; // 如果 slat是 05代表填上了 5 個bytes，5個都是05

	// 2.slast 不合理(0 或超過長度)直接視為無效 padding 
	if (slast === 0 || slast > buffer.length) {
		return false; 
	}

	// 3.取最後 $slast 個 byte 
	const pcheck = buffer.subarray(buffer.length - slast);

	// 4.檢查這 $slast 個 byte 是否「全部」都等於 slast 這個數值 
	let isValidPadding = true;
	for (let i = 0; i < pcheck.length; i++) { 
		if (pcheck[i] !== slast) { 
			isValidPadding = false;
			break;
		}
	}
	
	// 5.回傳檢查結果 
	if (isValidPadding) { 
		return buffers.slice(0, buffer.length - slast) // slice 不含 end 
		// return buffers.subarray(0, buffer.length - slast);
	} else { 
		return false; 
	} 
}

function opensslDecryptZeroPadding(data1, key, iv) {
	const encryptedBuffer = Buffer.from(data1, 'hex'); // 收到的資料是 16進位 
	
	const keyBuffer = Buffer.from(key, 'utf8');
	const ivBuffer = Buffer.from(iv, 'utf8'); 

	const decipher = crypto.createDecipheriv('aes-256-cbc', keyBuffer, ivBuffer); // 解密器
	decipher.setAutoPadding(false); // 對應 OPENSSL_ZERO_PADDING, 關閉自動 padding 處理

	const decrypted = Buffer.concat([ 
		decipher.update(encryptedBuffer), // 開始解密 
		decipher.final() 				  // 完成解密 
	]); 

	return decrypted; 
}

function genCheckCode(item, key, iv) { 

	// 1. ksort:依 key 名稱字母順序排序
	const sortedKeys = Object.keys(item).sort();
	
	// 2. http_build_query:組成 key=value1&key2=value2...
	// php 的 http_build_query 預設用 urlencode，空白會編成 +
	// node的 encodeURIComponent 空格彙編成 '%20'，'%20'換成 + 

	// encodeURIComponent 用來轉換特殊字元放入URL的字串中，如:hello world => hello%20world，將特殊字元轉換成 %XX
	const sortedObj = {};
	sortedKeys.forEach(k => { sortedObj[k] = item[k]; }); 
	const checkStr = querystring.stringify(sortedObj);
	 
	// 3. 組合 HashIV + 排序後的字串 + HashKey 
	const rawStr = `HashIV=${iv}&${checkStr}&HashKey=${key}`; 
	
	// 4. sha256 後轉大寫
	const code = crypto
		.createHash('sha256')
		.update(rawStr)
		.digest('hex')
		.toUpperCase(); 

	return code;
}



// 當商店發動單筆交易查詢(Query)時，需生成一組CheckValue 
function genCheckValue(params, key, iv) { 

	// var params = {
	// 	MerchantID: params.merchantID, 
	// 	Amt: params.amt,
	// 	MerchantOrderNo: params.merchantOrderNo, 
	// }; 	

	// 1. 依英文字母 A~Z 排序 key(這裡固定是 Amt、MerchantID、MerchantOrderNo)
	const sortedKeys = Object.keys(params).sort();

	// 2. 組成 key=value,用 & 串聯(注意:這裡官方範例沒有另外 urlencode,直接串值)
	const data1 = sortedKeys
		.map(k => `${k}=${params[k]}`) 
		.join('&');

	// 3. 前後補上 IV= 和 Key= 
	const hashs = `IV=${iv}&${data1}&Key=${key}`; 

	// 4. SHA256 後轉大寫 
	const CheckValue = crypto 
		.createHash('sha256') 
		.update(hashs)
		.digest('hex')
		.toUpperCase(); 
		
	return CheckValue; 
} 


function genMerchantOrderNo(){
	

// 	使用者點擊「選擇方案」
//        ↓
// 前端呼叫後端 API，建立一筆訂單（狀態：未付款）
//        ↓
// 後端把訂單資訊加密，回傳給前端
//        ↓
// 前端自動跳轉到藍新金流的付款頁面
//        ↓
// 使用者在藍新頁面填寫信用卡資訊、完成付款
//        ↓
// 藍新做兩件事（同時進行）：
//   1. NotifyURL：藍新「主動通知」你的後端付款成功
//      → 後端收到後更新訂單狀態為「已付款」，並建立購買記錄
//   2. ReturnURL：把使用者的瀏覽器「導回」你的網站
//      → 後端把使用者重新導向到前端的付款結果頁面

}

async function AddTradeInfo(tradeInfo){
	console.log("AddTradeInfo");

	// 從訂單編號獲得 user_id
	var user_id = tradeInfo.merchantOrderNo.split('_')[1];
	var data = Object.values(tradeInfo).concat([user_id]); 

	// 新
	let connection;
    try {
		connection = await connectionPool.promise().getConnection();
		await connection.beginTransaction(); 

        // 1. 寫入交易紀錄（無論成功失敗都記錄），用 ON DUPLICATE KEY UPDATE 防止重複 Notify 造成重複寫入
		// *如果是 PeriodStartType = 3，有 PeriodNo 但沒有 tradeNo，PeriodStartType 1, 2 才是都有
        const insertSql = `
            INSERT IGNORE INTO trade_info 
			(merchantID, merchantOrderNo, tradeNo, amt, payTime, status, type, periodNo, authTime, endDate, user_id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        `;  
		
        await connection.query(insertSql, [ 
            tradeInfo.merchantID,
            tradeInfo.merchantOrderNo,
            tradeInfo.tradeNo,
			tradeInfo.amt,
            tradeInfo.payTime,
            tradeInfo.status,
            tradeInfo.type,
			tradeInfo.periodNo,
			tradeInfo.authTime,
			tradeInfo.endDate,
            user_id,
        ]);

        // 2. 如果交易成功，更新使用者狀態 
        if (tradeInfo.status == 'SUCCESS') {
            await connection.query(
                'UPDATE user_info SET userStatus = ?, endDate = ? WHERE user_id = ?',
                [tradeInfo.userStatus, tradeInfo.endDate, user_id] 
            ); 
        } 

        await connection.commit(); 

    } catch (err) {
        if (connection) {
            await connection.rollback(); // 失敗全部退回 
        }
        console.error('Failed to process trade notify:', err); 
    
    } finally { 
        if (connection) {
            connection.release(); 
        }
    }
}

async function termSubInfo(tradeInfo){ 
	console.log("termSubInfo");

	// 從訂單編號獲得 user_id
	var user_id = tradeInfo.merchantOrderNo.split('_')[1];
	var data = Object.values(tradeInfo).concat([user_id]); 

	// 新
	let connection;
    try {
		connection = await connectionPool.promise().getConnection();
		await connection.beginTransaction(); 

        // 1. 寫入交易紀錄 
        const insertSql = `
            INSERT IGNORE INTO term_sub 
                (merchantOrderNo, periodNo, alterType, time, user_id) 
            VALUES (?, ?, ?, ?, ?)
        `;  
		
        await connection.query(insertSql, [ 
            tradeInfo.merchantOrderNo,
            tradeInfo.periodNo,
			tradeInfo.alterType,
            tradeInfo.time,
            user_id,
        ]);

        // 2. 如果交易成功，更新使用者狀態 
        if (tradeInfo.alterType == 'terminate') {
			// enum userStatus
			// {
			// 	terminate,  // 0 停止
			// 	activate,   // 1 啟用
			// 	pay,        // 2 付費
			// 	sub,        // 3 訂閱
			// 	unSub,      // 4 解除訂閱(還在訂閱期限內) 
			// }
            await connection.query(
				// 為了方便展示，所以改成立即中止，不然退訂是等到 滿月 才結算
				// 真正使用 不用改日期 只要改 userStatus = userStatus.unSub

                // 'UPDATE user_info SET userStatus = ? WHERE user_id = ?',
				'UPDATE user_info SET userStatus = ?, endDate = ? WHERE user_id = ?',
                [userStatus.unSub, "", user_id] 
            ); 	
        }

        await connection.commit(); 

    } catch (err) {
        if (connection) {
            await connection.rollback(); // 失敗全部退回 
        }
        console.error('Failed to process trade notify:', err); 
    
    } finally { 
        if (connection) {
            connection.release(); 
        }
    }
}

function genCreidtPayEndTime(){
	const date = new Date(Date.now() + 3 * 60 * 60 * 1000 + 24 * 60 * 60 * 1000 * 30);

	const result = new Intl.DateTimeFormat('sv-SE', {
		timeZone: 'Asia/Taipei',
		year: 'numeric',
		month: '2-digit',
		day: '2-digit',
		hour: '2-digit',
		minute: '2-digit',
		hourCycle: 'h23'
	}).format(date);

	return result; 
}

function genCreditPayTime(authTime){
	const str = authTime;

	// 解析 YYYYMMDDHHmmss
	const year = Number(str.substring(0, 4));
	const month = Number(str.substring(4, 6)) - 1;
	const day = Number(str.substring(6, 8));
	const hour = Number(str.substring(8, 10));
	const minute = Number(str.substring(10, 12));
	const second = Number(str.substring(12, 14));

	const date = new Date(year, month, day, hour, minute, second);

	// 加 30 天
	date.setDate(date.getDate() + 30);

	// 加 3 小時
	date.setHours(date.getHours() + 3);

	// YYYY-MM-DD HH:mm 
	const result =
		`${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ` +
		`${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;

	return result;
}

function solveTradeInfo(tradeInfo, test){
	
	// 1.解碼 notify 回來的交易訊息
	var res = stripPadding(opensslDecryptZeroPadding(tradeInfo, config.HASH_KEY, config.HASH_IV)); // 回傳的是 buffer
	const params = new URLSearchParams(res.toString('utf8')); 

	// 2.將資訊存入資料庫，無論交易成功或失敗都存 & 處理 user_info 的 userStatus
	var Info = {
		merchantID: params.get("MerchantID"), 
		merchantOrderNo: params.get("MerchantOrderNo"), 
		tradeNo: params.get("TradeNo"),
		payTime: params.get("PayTime"), 
		status: params.get("Status"),
		type: params.get("PaymentType"), // 交易類型: credit、linepay...
		periodNo: "",
		amt: params.get("Amt"), 
		authTime: 1,
		endDate: genCreidtPayEndTime(), // iso 
		userStatus: userStatus.pay
	};

	
	if(!test)
		AddTradeInfo(Info); // 更新資料庫 

	return Info;
}

function getAuthTime(authTime){
	if(authTime == "") // 首月免費的話，藍新不會傳授權時間，
		console.log("1");
	else
		return authTime;
}

// 信用卡定期定額 
function solvePeriodTrade(Period){
	console.log("solvePeriodTrade");

	// 1.解碼 notify 回來的交易訊息
	var res = stripPadding(opensslDecryptZeroPadding(Period, config.HASH_KEY, config.HASH_IV)); // 回傳的是 buffer
	const params = new URLSearchParams(res.toString('utf8')); 
	

	// (訂閱成功)將資訊存入資料庫，無論交易成功或失敗都存 & 處理 user_info 的 userStatus
	var Info = {
		merchantID: params.get("Result[MerchantID]")  ?? "", 
		merchantOrderNo: params.get("Result[MerchantOrderNo]")  ?? "", 
		tradeNo: params.get("Result[TradeNo]")  ?? "", // tradeNO & AuthTimes 如果沒有的話就代表，藍新授權成功，藍新沒有扣款，在我們這裡設定的是首月免費的方案(定期定額)
		payTime: params.get("Result[PayTime]") ?? "", 
		status: params.get("Status") ?? "",
		type: params.get("Result[PaymentMethod]") ?? "", // 交易類型: credit、linepay...
		periodNo: params.get("Result[PeriodNo]") ?? "",
		amt: params.get("Result[PeriodAmt]") ?? "", 
		authTime: params.get("Result[AuthTimes]") ?? "", 
		endDate: genCreidtPayEndTime(), // iso 
		userStatus: userStatus.sub, 
	};

	// 2.更新資料庫 
	AddTradeInfo(Info); 

	// 3.websocket 通知 pc 端
	var user_id = getUidFroMNO(Info.merchantOrderNo);
	sendToUser(user_id, { 
		op: Operation.subSW,
		user_id: user_id, 
		status: Info.status, 
	});	
}

function termPeriodTrade(Period){
	console.log("termPeriodTrade");

	// 1.解碼 notify 回來的交易訊息
	var res = stripPadding(opensslDecryptZeroPadding(Period, config.HASH_KEY, config.HASH_IV)); // 回傳的是 buffer
	const params = new URLSearchParams(res.toString('utf8')); 
	console.log(params);

	const date = new Date();
	const termTime =
		`${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')} ` +
		`${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;

	// 2. (取消訂閱) 
	var Info = {
		status: params.get("Status"), 
		message: params.get("Message"),
		merchantOrderNo: params.get("Result[MerOrderNo]" ?? ""), 
		periodNo: params.get("Result[PeriodNo]" ?? ""),
		alterType: params.get("Result[AlterType]" ?? ""),
		time: termTime, 
	}

	// 3.更新資料庫
	if(params.get("Result[status]") == "Success") 
		termSubInfo(Info);

	return Info;
	
}


// 交易完成後，收到來自藍新主機的通知
app.post('/api/chy/neWebpPay/notifyURL', upload.none(), function (req, res) {
	console.log('/api/chy/neWebpPay/notifyURL');

	// 要建立: 通用(id、訂單編號、userid、購買時間、金額、品項、是否付款)、非通(信用卡 PaymentMethod...)

	/// 信用卡一次付款
	if (Object.hasOwn(req.body, "TradeInfo")){
		// 1..後端資料建立，購買紀錄 * 開啟 userStatus 權限 & 判斷 user 是誰
		var Info = solveTradeInfo(req.body.TradeInfo.toString(), false); 
		var user_id = getUidFroMNO(Info.merchantOrderNo);

		// 2.通知 928 pc 交易是否成功
		sendToUser(user_id,  { 
			op: Operation.buySW, 
			user_id: user_id, 
			status: info.status, 
		});

	// Period 傳的是 multipart/form-data 要裝 multer 
	} else if(Object.hasOwn(req.body, "Period")){ // 訂閱 
		solvePeriodTrade(req.body.Period.toString()); 
		
	} else if(Object.hasOwn(req.body, "period")){ // 取消訂閱，會送到網頁
		// termPeriodTrade(req.body.period.toString());
	} 

	/// userInfo: userStatus(free, 一個月, 無限訂閱, 有限訂閱) + 到期時間。到時間之前，不可改方案，除非退費

}); 


app.post('/api/chy/neWebpPay/returnURL', function (req, res) {
	console.log('/api/chy/neWebpPay/returnURL');

	if(Object.hasOwn(req.body, "period")){ // 取消訂閱
		//termPeriodTrade(req.body.period.toString());
	} 

});

function sendToUser(user_id, data){ 
	console.log("\nwebsocket process message");
	
	const ws = clientMap.get(user_id); 

	if (!ws) {
		console.log(`not found: user ${user_id}`);
		return false;
	}

	if (ws.readyState !== WebSocket.OPEN){ 
		console.log(`close: user ${user_id}`); 
		clientMap.delete(user_id); 
		return false; 
	}

	console.log("websocket send to user"); 
	ws.send(JSON.stringify(data));
	return true; 
}

// 測試串接網址：https://ccore.newebpay.com/MPG/mpg_gateway 
// 正式串接網址：https://core.newebpay.com/MPG/mpg_gateway

// $result = NewebPay::result($request);
// $result->status()          // 交易狀態：'SUCCESS' 或錯誤代碼
// $result->isSuccess()       // 交易是否成功
// $result->isFail()          // 交易是否失敗
// $result->message()         // 交易狀態描述：'授權成功'
// $result->result()          // 回傳參數 (陣列)
// $result->merchantId()      // 藍新金流商店代號：'MS3311...'
// $result->amount()          // 交易金額：120
// $result->tradeNo()         // 藍新金流交易序號：'23061500000000000'
// $result->orderNo()         // 商店訂單編號：'1686759318'
// $result->paymentType()     // 付款方式：PaymentType::CREDIT
// $result->payTime()         // 支付完成時間：Carbon 實例
// $result->ip()              // 交易 IP：'127.0.0.1'
// $result->escrowBank()      // 款項保管銀行：'HNCB'

// Status=SUCCESS&
// Message=%E6%8E%88%E6%AC%8A%E6%88%90%E5%8A%9F&
// MerchantID=MS127874575&
// Amt=30&
// TradeNo=23092714215835071&
// MerchantOrderNo=Vanespl_ec_1695795668&
// RespondType=String&
// IP=123.51.237.115&
// EscrowBank=HNCB&
// PaymentType=CREDIT&
// RespondCode=00&
// Auth=115468&
// Card6No=400022&
// Card4No=1111&
// Exp=2609&
// AuthBank=KGI&
// TokenUseStatus=0&
// InstFirst=0&
// InstEach=0&
// Inst=0&
// ECI=&
// PayTime=2023-09-27+14%3A21%3A59&
// PaymentMethod=CREDIT
