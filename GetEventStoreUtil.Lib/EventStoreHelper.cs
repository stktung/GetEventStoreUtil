using System;

namespace GetEventStoreUtil
{
    public static class EventStoreHelper
    {
        public static Uri CreateConnectionUri(string host, int port, string username, string password)
        {
            //return new Uri("tcp://admin:changeit@localhost:1113");
            return new Uri($"tcp://{username}:{password}@{host}:{port}");
        }
    }
}