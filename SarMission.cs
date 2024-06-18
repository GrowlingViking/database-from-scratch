using System;

namespace database_from_scratch
{
    public class SarMission
    {
        public Guid Id { get; set; }
        public int AreaId { get; set; }
        public string Name { get; set; }
        public int Priority { get; set; }
        public bool IsFinished { get; set; }
    }
}