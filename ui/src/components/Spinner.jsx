export default function Spinner({ className = "" }) {
  return (
    <div className={`flex items-center justify-center py-20 ${className}`}>
      <div
        className="h-8 w-8 animate-spin rounded-full border-[3px] border-gray-200"
        style={{ borderTopColor: "var(--color-primary)" }}
      />
    </div>
  );
}
