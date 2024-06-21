namespace ETLConsoleApp.Models
{
    /// <summary>
    /// Represents the response from the API.
    /// </summary>
    public partial class ApiResponse
    {
        /// <summary>
        /// Gets or sets the application ID.
        /// </summary>
        public string AppId { get; set; } = null!;

        /// <summary>
        /// Gets or sets the page number.
        /// </summary>
        public long Pagenumber { get; set; }

        /// <summary>
        /// Gets or sets the page size.
        /// </summary>
        public long Pagesize { get; set; }

        /// <summary>
        /// Gets or sets the total record count.
        /// </summary>
        public string TotalRecordCount { get; set; } = null!;

        /// <summary>
        /// Gets or sets the status.
        /// </summary>
        public string Status { get; set; } = null!;

        /// <summary>
        /// Gets or sets the rows returned by the API.
        /// </summary>
        public Row[] Row { get; set; } = null!;
    }

    /// <summary>
    /// Represents a row in the API response.
    /// </summary>
    public partial class Row
    {
        /// <summary>
        /// Gets or sets the fields in the row.
        /// </summary>
        public Field[] Field { get; set; } = null!;
    }

    /// <summary>
    /// Represents a field in a row.
    /// </summary>
    public partial class Field
    {
        /// <summary>
        /// Gets or sets the name of the field.
        /// </summary>
        public Name Name { get; set; }

        /// <summary>
        /// Gets or sets the value of the field.
        /// </summary>
        public Value Value { get; set; }
    }

    /// <summary>
    /// Represents the possible names for a field.
    /// </summary>
    public enum Name
    {
        PERID,
        PERNR,
        ACTON,
        DYFIN,
        ESC02,
        LAST_UPDATE,
        NODES,
        OCCCL,
        SBDGT,
        FIELD1,
        FIELD2
    }

    /// <summary>
    /// Represents the value of a field, which can be either a long integer or a string.
    /// </summary>
    public partial struct Value
    {
        /// <summary>
        /// Gets or sets the integer value.
        /// </summary>
        public long? Integer { get; set; }

        /// <summary>
        /// Gets or sets the string value.
        /// </summary>
        public string String { get; set; }

        /// <summary>
        /// Implicitly converts a long integer to a Value.
        /// </summary>
        /// <param name="Integer">The integer value.</param>
        public static implicit operator Value(long Integer) => new Value { Integer = Integer };

        /// <summary>
        /// Implicitly converts a string to a Value.
        /// </summary>
        /// <param name="String">The string value.</param>
        public static implicit operator Value(string String) => new Value { String = String };
    }
}