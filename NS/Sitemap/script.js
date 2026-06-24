// Handle view switching
document.querySelectorAll('.view-option').forEach(option => {
    option.addEventListener('click', () => {
        // Remove active class from all options and views
        document.querySelectorAll('.view-option').forEach(opt => opt.classList.remove('active'));
        document.querySelectorAll('.view').forEach(view => view.classList.remove('active'));

        // Add active class to clicked option and corresponding view
        option.classList.add('active');
        const viewId = option.getAttribute('data-view') + '-view';
        document.getElementById(viewId).classList.add('active');
    });
});

