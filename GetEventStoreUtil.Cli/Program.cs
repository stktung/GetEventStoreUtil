using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using EventStore.ClientAPI.Exceptions;

namespace GetEventStoreUtil.Cli
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Enter stream category to delete:");
            var streamCategory = Console.ReadLine();
            var conn = EventStoreConnection.Create(new Uri("tcp://admin:changeit@localhost:1113"), "GetEventStoreUtil");
            await conn.ConnectAsync();

            await DeleteStreamsByCategory(conn, streamCategory);
        }
        public static async Task DeleteStreamsByCategory(IEventStoreConnection conn, string category, bool hardDelete = false)
        {
            IEnumerable<string> streamIds = await GetStreamIdsByCategoryBy(conn, category);
            
            if (!streamIds.Any())
            {
                Console.WriteLine("INFO: No stream was found. Exitting.");
                return;
            }

            Console.WriteLine($"[{streamIds.Count()}] streams for category [{category}] will be deleted. Are you sure? (press any key to confirm or ctrl-c to exit)");
            Console.ReadLine();

            foreach (var id in streamIds)
            {
                try
                {
                    await conn.DeleteStreamAsync(id, ExpectedVersion.Any, hardDelete);
                    Console.WriteLine($"DEBUG: [{id}]");
                }
                catch (StreamDeletedException e)
                {
                    Console.WriteLine($"INFO: Stream [{e.Stream} is already deleted. Skipped]");
                }
            }

            Console.WriteLine("INFO: Success");
        }

        private static async Task<IEnumerable<string>> GetStreamIdsByCategoryBy(IEventStoreConnection conn, string category, bool includeDeleted = false)
        {
            var streamIds = await GetAllRawEvents(conn, "$category-" + category);
            
            if (includeDeleted)
            {
                return streamIds;
            }
            else
            {
                var undeletedStreamIds = new List<string>();   
                foreach (var id in streamIds)
                {
                    var result = await conn.GetStreamMetadataAsync(id);
                    if (!result.IsStreamDeleted) undeletedStreamIds.Add(id);
                }
                return undeletedStreamIds;
            }
        }

        private static async Task<IEnumerable<string>> GetAllRawEvents(IEventStoreConnection conn, string streamId)
        {
            var streamEvents = await GetAllResolvedEvents(conn, streamId);

            return streamEvents.Select(e => Encoding.UTF8.GetString(e.Event.Data));
        }

        private static async Task<IEnumerable<ResolvedEvent>> GetAllResolvedEvents(IEventStoreConnection conn, string streamId)
        {
            var streamEvents = new List<ResolvedEvent>();

            StreamEventsSlice currentSlice;
            long nextSliceStart = StreamPosition.Start;
            do
            {
                currentSlice = await conn.ReadStreamEventsForwardAsync(streamId, nextSliceStart,
                    ClientApiConstants.MaxReadSize, false);
                nextSliceStart = currentSlice.NextEventNumber;
                streamEvents.AddRange(currentSlice.Events);
            } while (!currentSlice.IsEndOfStream);

            return streamEvents;
        }
    }
}
