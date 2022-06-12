package music_recommend
// artist_data_small.txt : アーティスト名と ID
// artist_alias_small.txt : アーティストのエイリアス（別々の名前がついているが同じアーティスト）
// user_artist_data_small.txt : メインデータ（ユーザーのアーティストの曲の再生回数、アーティスト単位）

// 協調フィルタリング：２つのエンティティ間において、あるデータの類似性から、他のデータにおいても類似性を判定する方法
// i.e. 二人のユーザーが同じ曲を好きであるかどうかを、他の共通の曲をたくさん再生しているかどうかから判定
// tags: #レコメンデーション #協調フィルタリング #因子分析 #非負値行列因子分析 #non-negative_matrix_factorization #行列補完アルゴリズム
class MusicRecommend {}
