INSERT INTO energy_action_logs (
  action_name,
  address_base58,
  provider,
  energy_amount,
  action_score,
  status,
  response_body,
  error_message,
  created_at
) VALUES
(
  '发能一次',
  'TQDemoAddr1111111111111111111111111',
  'trxfee',
  65000,
  1,
  'SUCCESS',
  '{"message":"demo success 1"}',
  NULL,
  CONCAT(DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d'), ' 09:10:00')
),
(
  '发能两次',
  'TQDemoAddr2222222222222222222222222',
  'trxfee',
  130000,
  2,
  'SUCCESS',
  '{"message":"demo success 2"}',
  NULL,
  CONCAT(DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d'), ' 10:25:00')
),
(
  '发能一次',
  'TQDemoAddr3333333333333333333333333',
  'catfee',
  65000,
  1,
  'SUCCESS',
  '{"message":"demo success 3"}',
  NULL,
  CONCAT(DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d'), ' 11:40:00')
),
(
  '发能两次',
  'TQDemoAddr4444444444444444444444444',
  'trxfee',
  130000,
  2,
  'SUCCESS',
  '{"message":"demo success 4"}',
  NULL,
  CONCAT(DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d'), ' 13:05:00')
),
(
  '发能一次',
  'TQDemoAddr5555555555555555555555555',
  'catfee',
  65000,
  1,
  'SUCCESS',
  '{"message":"demo success 5"}',
  NULL,
  CONCAT(DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d'), ' 16:20:00')
),
(
  '发能两次',
  'TQDemoAddr6666666666666666666666666',
  'trxfee',
  130000,
  2,
  'SUCCESS',
  '{"message":"demo success 6"}',
  NULL,
  CONCAT(DATE_FORMAT(DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY), '%Y-%m-%d'), ' 08:30:00')
),
(
  '发能一次',
  'TQDemoAddr7777777777777777777777777',
  'catfee',
  65000,
  1,
  'SUCCESS',
  '{"message":"demo success 7"}',
  NULL,
  CONCAT(DATE_FORMAT(DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY), '%Y-%m-%d'), ' 10:15:00')
),
(
  '发能两次',
  'TQDemoAddr8888888888888888888888888',
  'trxfee',
  130000,
  2,
  'SUCCESS',
  '{"message":"demo success 8"}',
  NULL,
  CONCAT(DATE_FORMAT(DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY), '%Y-%m-%d'), ' 12:45:00')
),
(
  '发能一次',
  'TQDemoAddr9999999999999999999999999',
  'catfee',
  65000,
  1,
  'SUCCESS',
  '{"message":"demo success 9"}',
  NULL,
  CONCAT(DATE_FORMAT(DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY), '%Y-%m-%d'), ' 15:10:00')
),
(
  '发能两次',
  'TQDemoAddrAAAAAAAAAAAAAAAAAAAAAAAAA',
  'trxfee',
  130000,
  2,
  'SUCCESS',
  '{"message":"demo success 10"}',
  NULL,
  CONCAT(DATE_FORMAT(DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY), '%Y-%m-%d'), ' 18:35:00')
);
