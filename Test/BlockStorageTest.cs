using database_from_scratch.Core;

namespace Test;

[TestFixture]
public class BlockStorageTest
{
    [Test]
    public void TestBlockStoragePersistent()
    {
        using var ms = new MemoryStream();
        
        var storage = new BlockStorage(ms);

        using (var firstBlock = storage.CreateNew())
        using (var secondBlock = storage.CreateNew())
        using (var thirdBlock = storage.CreateNew())
        {
            Assert.Multiple(() =>
            {
                Assert.That(firstBlock.Id, Is.EqualTo(0));
                Assert.That(secondBlock.Id, Is.EqualTo(1));
            });

            secondBlock.SetHeader(1, 100);
            secondBlock.SetHeader(2, 200);
            Assert.Multiple(() =>
            {
                Assert.That(thirdBlock.Id, Is.EqualTo(2));
                Assert.That(ms.Length, Is.EqualTo(storage.BlockSize * 3));
            });
        }
        
        // Test to make sure out creation persists
        var storage2 = new BlockStorage(ms);
        Assert.Multiple(() =>
        {
            Assert.That(storage2.Get(0).Id, Is.EqualTo(0));
            Assert.That(storage2.Get(1).Id, Is.EqualTo(1));
            Assert.That(storage2.Get(2).Id, Is.EqualTo(2));
            
            Assert.That(storage2.Get(1).GetHeader(1), Is.EqualTo(100));
            Assert.That(storage2.Get(1).GetHeader(2), Is.EqualTo(200));
        });
    }
}