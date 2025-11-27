import React from 'react';

type PrintProgressProps = {
    isVisible: boolean;
    results: Array<{
        order_id: string;
        status: 'success' | 'error' | 'processing';
        message: string;
        step?: string;
        cloudprinter_reference?: string;
    }>;
};

const getStepColor = (step?: string) => {
    switch (step) {
        case 'completed':
            return 'text-green-600';
        case 'database_lookup':
            return 'text-blue-600';
        case 'cloudprinter_api':
            return 'text-purple-600';
        case 'processing':
            return 'text-orange-600';
        case 'queued':
            return 'text-gray-400';
        default:
            return 'text-gray-600';
    }
};

const getStepEmoji = (step?: string) => {
    switch (step) {
        case 'completed':
            return '✅';
        case 'database_lookup':
            return '🔍';
        case 'cloudprinter_api':
            return '🖨️';
        case 'processing':
            return '⚙️';
        case 'queued':
            return '⏳';
        default:
            return '📋';
    }
};

const getStatusColor = (status: 'success' | 'error' | 'processing') => {
    switch (status) {
        case 'success':
            return 'bg-green-50 border-green-200';
        case 'error':
            return 'bg-red-50 border-red-200';
        case 'processing':
            return 'bg-blue-50 border-blue-200';
        default:
            return 'bg-gray-50 border-gray-200';
    }
};

const getStatusBadgeColor = (status: 'success' | 'error' | 'processing') => {
    switch (status) {
        case 'success':
            return 'bg-green-100 text-green-800';
        case 'error':
            return 'bg-red-100 text-red-800';
        case 'processing':
            return 'bg-blue-100 text-blue-800';
        default:
            return 'bg-gray-100 text-gray-800';
    }
};

const PrintProgress: React.FC<PrintProgressProps> = ({ isVisible, results }) => {
    if (!isVisible) return null;

    const processingCount = results.filter(r => r.status === 'processing').length;
    const completedCount = results.filter(r => r.status === 'success').length;
    const failedCount = results.filter(r => r.status === 'error').length;

    return (
        <div className="fixed bottom-4 right-4 w-96 bg-white rounded-lg shadow-lg p-4 max-h-96 overflow-y-auto">
            <div className="flex items-center justify-between mb-3">
                <h3 className="text-lg font-semibold">Print Progress</h3>
                <div className="text-sm text-gray-500">
                    {processingCount > 0 && <span>{processingCount} processing • </span>}
                    {completedCount > 0 && <span>{completedCount} completed • </span>}
                    {failedCount > 0 && <span>{failedCount} failed</span>}
                </div>
            </div>
            <div className="space-y-3">
                {results.map((result, index) => (
                    <div
                        key={`${result.order_id}-${index}`}
                        className={`p-3 rounded-md ${getStatusColor(result.status)}`}
                    >
                        <div className="flex items-center justify-between mb-2">
                            <span className="font-medium">{result.order_id}</span>
                            <span
                                className={`px-2 py-1 text-xs rounded ${getStatusBadgeColor(result.status)}`}
                            >
                                {result.status.toUpperCase()}
                            </span>
                        </div>
                        {result.step && (
                            <div className={`text-sm ${getStepColor(result.step)} flex items-center gap-2 mt-1`}>
                                <span>{getStepEmoji(result.step)}</span>
                                <span>{result.step.replace(/_/g, ' ').toUpperCase()}</span>
                            </div>
                        )}
                        <div className="text-sm mt-2 text-gray-700">
                            {result.message}
                        </div>
                        {result.cloudprinter_reference && (
                            <div className="text-sm text-blue-600 mt-2 flex items-center gap-2">
                                <span>🔖</span>
                                <span>Reference: {result.cloudprinter_reference}</span>
                            </div>
                        )}
                    </div>
                ))}
            </div>
        </div>
    );
};

export default PrintProgress; 