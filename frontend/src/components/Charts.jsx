import { useEffect, useRef } from 'react'

const CATEGORY_COLORS = {
  Food:          '#e8855a',
  Investment:    '#c9a84c',
  Transfer:      '#5b8dee',
  Health:        '#3ecf8e',
  Subscription:  '#a855f7',
  Shopping:      '#f06292',
  Transport:     '#26c6da',
  Utilities:     '#78909c',
  Groceries:     '#66bb6a',
  Entertainment: '#ff7043',
  Education:     '#ab47bc',
  Payments:      '#8d6e63',
  Other:         '#546e7a',
}

export function DonutChart({ data, onSliceClick }) {
  const canvasRef = useRef(null)
  const chartRef  = useRef(null)

  useEffect(() => {
    if (!data?.length || !canvasRef.current) return

    const loadChart = async () => {
      const { Chart, ArcElement, DoughnutController, Tooltip, Legend } = await import('chart.js')
      Chart.register(ArcElement, DoughnutController, Tooltip, Legend)

      if (chartRef.current) chartRef.current.destroy()

      const labels = data.map(d => d.category)
      const values = data.map(d => d.total_amount)
      const colors = labels.map(l => CATEGORY_COLORS[l] || CATEGORY_COLORS.Other)

      chartRef.current = new Chart(canvasRef.current, {
        type: 'doughnut',
        data: {
          labels,
          datasets: [{
            data: values,
            backgroundColor: colors.map(c => c + '22'),
            borderColor: colors,
            borderWidth: 1.5,
            hoverBackgroundColor: colors.map(c => c + '44'),
            hoverBorderWidth: 2,
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          cutout: '70%',
          plugins: {
            legend: { display: false },
            tooltip: {
              backgroundColor: '#1c1c28',
              borderColor: 'rgba(255,255,255,0.08)',
              borderWidth: 1,
              titleColor: '#f0ede8',
              bodyColor: 'rgba(240,237,232,0.6)',
              padding: 12,
              callbacks: {
                label: (ctx) => `  ₹${ctx.raw.toLocaleString('en-IN', { maximumFractionDigits: 0 })}`
              }
            }
          },
          onClick: (_, elements) => {
            if (elements.length && onSliceClick) {
              onSliceClick(data[elements[0].index])
            }
          }
        }
      })
    }

    loadChart()
    return () => { if (chartRef.current) chartRef.current.destroy() }
  }, [data])

  return <canvas ref={canvasRef} />
}

export function BarChart({ data }) {
  const canvasRef = useRef(null)
  const chartRef  = useRef(null)

  useEffect(() => {
    if (!data?.length || !canvasRef.current) return

    const loadChart = async () => {
      const { Chart, BarElement, BarController, CategoryScale, LinearScale, Tooltip } = await import('chart.js')
      Chart.register(BarElement, BarController, CategoryScale, LinearScale, Tooltip)

      if (chartRef.current) chartRef.current.destroy()

      const labels = data.map(d => d.category)
      const values = data.map(d => d.total_amount)
      const colors = labels.map(l => CATEGORY_COLORS[l] || CATEGORY_COLORS.Other)

      chartRef.current = new Chart(canvasRef.current, {
        type: 'bar',
        data: {
          labels,
          datasets: [{
            data: values,
            backgroundColor: colors.map(c => c + '33'),
            borderColor: colors,
            borderWidth: 1,
            borderRadius: 6,
            hoverBackgroundColor: colors.map(c => c + '55'),
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              backgroundColor: '#1c1c28',
              borderColor: 'rgba(255,255,255,0.08)',
              borderWidth: 1,
              titleColor: '#f0ede8',
              bodyColor: 'rgba(240,237,232,0.6)',
              padding: 12,
              callbacks: {
                label: (ctx) => `  ₹${ctx.raw.toLocaleString('en-IN', { maximumFractionDigits: 0 })}`
              }
            }
          },
          scales: {
            x: {
              grid: { color: 'rgba(255,255,255,0.04)' },
              ticks: { color: 'rgba(240,237,232,0.4)', font: { family: 'DM Mono', size: 11 } },
              border: { color: 'rgba(255,255,255,0.06)' },
            },
            y: {
              grid: { color: 'rgba(255,255,255,0.04)' },
              ticks: {
                color: 'rgba(240,237,232,0.4)',
                font: { family: 'DM Mono', size: 11 },
                callback: v => '₹' + (v >= 1000 ? (v/1000).toFixed(0) + 'k' : v)
              },
              border: { color: 'rgba(255,255,255,0.06)' },
            }
          }
        }
      })
    }

    loadChart()
    return () => { if (chartRef.current) chartRef.current.destroy() }
  }, [data])

  return <canvas ref={canvasRef} />
}

export { CATEGORY_COLORS }