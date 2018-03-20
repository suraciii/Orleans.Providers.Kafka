using System;
using System.Collections.Generic;
using System.Text;

namespace Tester
{
    public static class EventBusConstants
    {
        public const string EVENT_BUS_PROVIDER = "EventBus";
        public const string DOMAIN_EVENT_TEST_TOPIC = "Test.DomainSample";

    }

    public static class DomainEventConstants
    {
        public const string PublishEventFromClient = nameof(PublishEventFromClient);
    }
}
