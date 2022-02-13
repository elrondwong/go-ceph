package rados

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func (suite *RadosTestSuite) TestWatch() {
	suite.SetupConnection()

	ioctx, err := suite.conn.OpenIOContext(suite.pool)
	require.NoError(suite.T(), err)

	oid := suite.GenObjectName()
	defer func(oid string) {
		require.NoError(suite.T(), ioctx.Delete(oid))
	}(oid)
	err = ioctx.Write(oid, []byte(oid), 0)
	require.NoError(suite.T(), err)

	watch, err := ioctx.Watch(oid,
		func(arg interface{}, notifyID uint64, handle uint64, notifierID uint64, data interface{}, dataLen uint64) {
			err = ioctx.NotifyAck(oid, notifyID, handle, nil)
			require.NoError(suite.T(), err)
		},
		func(arg interface{}, cookie uint64, err int) {
			require.NotNil(suite.T(), cookie)
		}, 30, nil)
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), watch)

	suite.T().Run("send notify", func(t *testing.T) {
		err := ioctx.Notify(oid, []byte("abracadabra"), 30)
		require.NoError(t, err)
	})

	suite.T().Run("flush watch", func(t *testing.T) {
		err := suite.conn.WatchFlush()
		require.NoError(t, err)
	})

	suite.T().Run("shutdown watch", func(t *testing.T) {
		err := watch.UnWatch()
		require.NoError(t, err)
	})
}
