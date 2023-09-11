# Calc_GTFS_RT_Delay

## 概要
GTFS_RTデータから遅延情報を算出します。

## 外れ値  
なぜか関東自動車のGTFS-RTデータには同じIDが２回使用されている事がある。
その時は当然、遅延時刻が膨大になってしまう。

dateは運行日ではなく、深夜バスなどは前日扱いになる

エラー発生履歴
関東自動車 8/27 8/28 tripIDが存在しない